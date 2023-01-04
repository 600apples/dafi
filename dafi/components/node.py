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
from dafi.exceptions import UnableToFindCandidate, RemoteStoppedUnexpectedly
from dafi.utils.settings import LOCAL_CALLBACK_MAPPING, WELL_KNOWN_CALLBACKS
from dafi.components.operations.node_operations import NodeOperations
from dafi.components.operations.channel_store import ChannelPipe, MessageIterator
from dafi.exceptions import ReckAcceptError


class Node(ComponentsBase):

    async def _handle(self, task_status: TaskStatus):
        self.logger = patch_logger(logging.getLogger(__name__), colors.green)

        async with self.connect_listener() as aio_listener:
            stub = grpc_messager.MessagerServiceStub(aio_listener)
            message_iterator = MessageIterator(global_terminate_event=self.global_terminate_event)
            self.channel = ChannelPipe(
                send_iterator=message_iterator, receive_iterator=stub.communicate(message_iterator)
            )
            if not self.operations:
                self.operations = NodeOperations(logger=self.logger)
            self.operations.set_channel(self.channel)

            async with create_task_group() as sg:
                self.scheduler = Scheduler(process_name=self.process_name, sg=sg)
                sg.start_soon(self.read_commands, task_status, sg)
                sg.start_soon(self.reconnect_request)
                self.register_on_stop_callback(self.on_node_stop)

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

    @with_debug_trace
    async def on_node_stop(self) -> NoReturn:
        await self.scheduler.on_scheduler_stop()
        self.channel.send_iterator.stop()
        LOCAL_CALLBACK_MAPPING.clear()

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
                raise ReckAcceptError()

        await self.operations.on_channel_close()
        if not self.global_terminate_event.is_set():
            raise ReckAcceptError()

    @with_debug_trace
    def send_threadsave(self, msg: RpcMessage, eta: Union[int, float]):
        self.channel.send_threadsave(msg, eta)

    def send(self, msg: RpcMessage, eta: Union[int, float]):
        self.channel.send(msg, eta)

    @with_debug_trace
    def register_result(self, result: Optional[AsyncResult]) -> NoReturn:
        if result:
            AsyncResult._awaited_results[result.uuid] = result
