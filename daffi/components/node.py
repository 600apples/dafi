from typing import Union, NoReturn
from anyio import create_task_group
from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus

from daffi.utils import colors

from daffi.utils.logger import get_daffi_logger
from daffi.async_result import AsyncResult
from daffi.components import ComponentsBase
from daffi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag

from daffi.components.scheduler import Scheduler
from daffi.components.proto import messager_pb2_grpc as grpc_messager
from daffi.exceptions import (
    UnableToFindCandidate,
    RemoteStoppedUnexpectedly,
    InitializationError,
)
from daffi.settings import LOCAL_CALLBACK_MAPPING, WELL_KNOWN_CALLBACKS
from daffi.components.operations.node_operations import NodeOperations
from daffi.components.operations.channel_store import ChannelPipe, MessageIterator, FreezableQueue


class Node(ComponentsBase):

    # ------------------------------------------------------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def node_callback_mapping(self):
        return self.operations.node_callback_mapping

    @property
    def stream_store(self):
        return self.operations.stream_store

    # ------------------------------------------------------------------------------------------------------------------
    # Node lifecycle ( on_init -> before_connect -> on_stop )
    # ------------------------------------------------------------------------------------------------------------------

    async def on_init(self) -> NoReturn:
        process_ident = f"{self.__class__.__name__.lower()}[{self.process_name}]"
        self.logger = get_daffi_logger(process_ident, colors.green)
        self.operations = NodeOperations(logger=self.logger, async_backend=self.async_backend)
        self.scheduler = Scheduler(process_name=self.process_name, async_backend=self.async_backend)

    async def on_stop(self) -> NoReturn:
        await super().on_stop()
        self.logger.debug(f"On stop event triggered")
        await self.scheduler.on_scheduler_stop()
        self.operations.on_remote_error()

        if self.channel:
            await self.channel.clear_queue()
            await self.channel.stop()
        FreezableQueue.factory_remove(self.ident)
        self.logger.info(f"{self.__class__.__name__} stopped.")
        self._stopped = True

    async def before_connect(self) -> NoReturn:
        if not getattr(self, "channel", None):
            FreezableQueue.factory_remove(self.ident)
            self.channel = None

    def on_error(self) -> NoReturn:
        if channel := getattr(self, "channel", None):
            channel.freeze(10)
        self.operations.on_remote_error()

    # ------------------------------------------------------------------------------------------------------------------
    # Message operations
    # ------------------------------------------------------------------------------------------------------------------

    async def handle_operations(self, task_status: TaskStatus) -> NoReturn:

        async with self.connect_listener() as aio_listener:
            stub = grpc_messager.MessagerServiceStub(aio_listener)

            message_iterator = MessageIterator(await FreezableQueue.factory(self.ident))
            receive_iterator = stub.communicate(message_iterator, metadata=[("ident", self.ident)])

            self.channel = ChannelPipe(
                send_iterator=message_iterator, receive_iterator=receive_iterator, ident=self.ident
            )
            self.operations.channel = self.channel

            async with create_task_group() as sg:
                sg.start_soon(self.read_commands, task_status, sg, stub)
                sg.start_soon(self.read_streams, sg, stub)

    async def read_commands(self, task_status: TaskStatus, sg: TaskGroup, stub):
        local_mapping_without_well_known_callbacks = {
            k: v.simplified() for k, v in LOCAL_CALLBACK_MAPPING.items() if k not in WELL_KNOWN_CALLBACKS
        }
        await self.channel.send(
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

            elif msg.flag == MessageFlag.STOP_REQUEST:
                await self.operations.on_stop_request()

            elif msg.flag == MessageFlag.INIT_STREAM:
                await self.operations.on_stream_init(msg, stub, sg, self.process_name)

            elif msg.flag == MessageFlag.STREAM_ERROR:
                await self.operations.on_stream_error(msg)

            elif msg.flag == MessageFlag.STREAM_THROTTLE:
                await self.operations.on_stream_throttle(msg)

            elif msg.flag == MessageFlag.CONTROLLER_STOPPED_UNEXPECTEDLY:
                self.operations.on_remote_error()

    def send_threadsave(self, msg: RpcMessage, eta: Union[int, float]):
        """Send message outside of node executor scope."""
        self.channel.send_threadsave(msg, eta)

    def send_and_register_result(self, msg: RpcMessage) -> AsyncResult:
        if msg.return_result is False:
            InitializationError(
                f"Unable to register result for callback {msg.func_name}. return_result=False specified in message"
            ).fire()
        result = AsyncResult(msg=msg)._register()
        self.send_threadsave(msg, 0)
        return result

    # ------------------------------------------------------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------------------------------------------------------

    async def read_streams(self, sg: TaskGroup, stub):
        while True:
            stream_pair_group, receivers, msg_uuid = await self.stream_store.accept_multi_connection()
            for stream_pair, receiver in zip(stream_pair_group, receivers):
                sg.start_soon(self.fire_stream, stream_pair, msg_uuid, receiver, stub)

    async def fire_stream(self, stream_pair, msg_uuid: str, receiver: str, stub):
        await stub.stream_to_controller(stream_pair, metadata=[("receiver", receiver), ("uuid", msg_uuid)])
