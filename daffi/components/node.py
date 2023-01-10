import logging
from typing import Union, Optional, NoReturn

from anyio import create_task_group, sleep
from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus

from daffi.utils import colors

from daffi.utils.logger import patch_logger
from daffi.async_result import AsyncResult
from daffi.components import ComponentsBase
from daffi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag
from daffi.utils.debug import with_debug_trace
from daffi.components.scheduler import Scheduler
from daffi.components.proto import messager_pb2_grpc as grpc_messager
from daffi.exceptions import UnableToFindCandidate, RemoteStoppedUnexpectedly, InitializationError, RemoteError
from daffi.utils.settings import LOCAL_CALLBACK_MAPPING, WELL_KNOWN_CALLBACKS
from daffi.components.operations.node_operations import NodeOperations
from daffi.components.operations.channel_store import ChannelPipe, MessageIterator, FreezableQueue


class Node(ComponentsBase):

    # ------------------------------------------------------------------------------------------------------------------
    # Node lifecycle ( on_init -> before_connect -> on_stop )
    # ------------------------------------------------------------------------------------------------------------------

    async def on_init(self) -> NoReturn:
        self.logger = patch_logger(logging.getLogger(__name__), colors.green)
        self.operations = NodeOperations(logger=self.logger, async_backend=self.async_backend)

    @with_debug_trace
    async def on_stop(self) -> NoReturn:
        if self.channel:
            await self.channel.clear_queue()
            await self.channel.stop()
        FreezableQueue.factory_remove(self.ident)
        await super().on_stop()

    async def before_connect(self) -> NoReturn:
        self.channel = None
        for msg_uuid, ares in AsyncResult._awaited_results.items():
            if isinstance(ares, AsyncResult):
                AsyncResult._awaited_results[msg_uuid] = RemoteError(
                    info="Lost connection to Controller.",
                    _awaited_error_type=RemoteStoppedUnexpectedly,
                )
                ares._set()

    # ------------------------------------------------------------------------------------------------------------------

    @property
    def node_callback_mapping(self):
        return self.operations.node_callback_mapping

    async def handle_operations(self, task_status: TaskStatus) -> NoReturn:

        async with self.connect_listener() as aio_listener:
            stub = grpc_messager.MessagerServiceStub(aio_listener)

            message_iterator = MessageIterator(FreezableQueue.factory(self.ident))
            receive_iterator = stub.communicate(message_iterator, metadata=[("ident", self.ident)])

            self.channel = ChannelPipe(send_iterator=message_iterator, receive_iterator=receive_iterator)
            self.operations.channel = self.channel

            async with create_task_group() as sg:
                self.scheduler = Scheduler(process_name=self.process_name, sg=sg, async_backend=self.async_backend)
                sg.start_soon(self.read_commands, task_status, sg)
                sg.start_soon(self.reconnect_request)

    async def reconnect_request(self):
        if self.reconnect_freq:
            while True:
                await sleep(self.reconnect_freq)
                self.channel.send(
                    ServiceMessage(
                        flag=MessageFlag.RECK_REQUEST,
                        transmitter=self.process_name,
                    )
                )

    async def read_commands(self, task_status: TaskStatus, sg: TaskGroup):

        local_mapping_without_well_known_callbacks = {
            k: v for k, v in LOCAL_CALLBACK_MAPPING.items() if k not in WELL_KNOWN_CALLBACKS
        }
        self.channel.send(
            ServiceMessage(
                flag=MessageFlag.HANDSHAKE,
                transmitter=self.process_name,
                data=local_mapping_without_well_known_callbacks,
            )
        )

        async for msg in self.channel:
            if msg.flag in (MessageFlag.HANDSHAKE, MessageFlag.UPDATE_CALLBACKS):
                await self.operations.on_handshake(msg, task_status, self.process_name, self.info)

            elif msg.flag in (MessageFlag.REQUEST, MessageFlag.BROADCAST):
                await self.operations.on_request(msg, sg, self.process_name, self.scheduler)

            elif msg.flag == MessageFlag.SUCCESS:
                await self.operations.on_success(msg, self.scheduler)

            elif msg.flag == MessageFlag.SCHEDULER_ACCEPT:
                await self.operations.on_scheduler_accept(msg, self.process_name)

            elif msg.flag in (
                MessageFlag.UNABLE_TO_FIND_CANDIDATE,
                MessageFlag.UNABLE_TO_FIND_PROCESS,
                MessageFlag.REMOTE_STOPPED_UNEXPECTEDLY,
            ):
                msg.loads()
                if msg.flag == MessageFlag.REMOTE_STOPPED_UNEXPECTEDLY:
                    msg.error._awaited_error_type = RemoteStoppedUnexpectedly
                else:
                    msg.error._awaited_error_type = UnableToFindCandidate
                await self.operations.on_unable_to_find(msg)

            elif msg.flag == MessageFlag.RECK_ACCEPT:
                await self.operations.on_reconnection()

            elif msg.flag == MessageFlag.STOP_REQUEST:
                await self.operations.on_stop_request(msg, self.process_name)

        if not self.global_terminate_event.is_set():
            await self.operations.on_reconnection()

    @with_debug_trace
    def send_threadsave(self, msg: RpcMessage, eta: Union[int, float]):
        """Send message outside of node executor scope."""
        self.channel.send_threadsave(msg, eta)

    def send(self, msg: RpcMessage, eta: Union[int, float]):
        """Send message inside of node executor scope."""
        self.channel.send(msg, eta)

    def send_and_register_result(self, func_name: str, msg: RpcMessage) -> AsyncResult:
        if msg.return_result is False:
            raise InitializationError(
                f"Unable to register result for callback {func_name}. return_result=False specified in message"
            )
        result = AsyncResult(func_name=func_name, uuid=msg.uuid)._register()
        self.send_threadsave(msg, 0)
        return result
