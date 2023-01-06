import logging
from typing import Union, Optional, NoReturn

from anyio import create_task_group, sleep
from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus

from dafi.utils import colors

from dafi.utils.logger import patch_logger
from dafi.async_result import AsyncResult
from dafi.components import ComponentsBase
from dafi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag
from dafi.utils.debug import with_debug_trace
from dafi.components.scheduler import Scheduler
from dafi.components.proto import messager_pb2_grpc as grpc_messager
from dafi.exceptions import UnableToFindCandidate, RemoteStoppedUnexpectedly, InitializationError
from dafi.utils.settings import LOCAL_CALLBACK_MAPPING, WELL_KNOWN_CALLBACKS
from dafi.components.operations.node_operations import NodeOperations
from dafi.components.operations.channel_store import ChannelPipe, MessageIterator, FreezableQueue


class Node(ComponentsBase):

    # ------------------------------------------------------------------------------------------------------------------
    # Node lifecycle ( on_init -> before_connect -> on_stop )
    # ------------------------------------------------------------------------------------------------------------------

    async def on_init(self) -> NoReturn:
        self.logger = patch_logger(logging.getLogger(__name__), colors.green)
        self.operations = NodeOperations(logger=self.logger)

    @with_debug_trace
    async def on_stop(self) -> NoReturn:
        await self.channel.clear_queue()
        await self.channel.stop()
        FreezableQueue.factory_remove(self.ident)
        await super().on_stop()

    async def before_connect(self) -> NoReturn:
        self.channel = None
        await AsyncResult._fold_results()

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
                self.scheduler = Scheduler(process_name=self.process_name, sg=sg)
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
        self.channel.send_threadsave(msg, eta)

    def send(self, msg: RpcMessage, eta: Union[int, float]):
        self.channel.send(msg, eta)

    @with_debug_trace
    def register_result(self, result: Optional[AsyncResult]) -> NoReturn:
        if result:
            AsyncResult._awaited_results[result.uuid] = result

    def send_and_register_result(self, func_name: str, msg: RpcMessage) -> AsyncResult:
        if msg.return_result is False:
            raise InitializationError(
                f"Unable to register result for callback {func_name}. return_result=False specified in message"
            )
        result = AsyncResult(
            func_name=func_name,
            uuid=msg.uuid,
        )
        self.register_result(result)
        self.send_threadsave(msg, 0)
        return result
