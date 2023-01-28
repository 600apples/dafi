import logging
import asyncio
from typing import NoReturn

from anyio.abc import TaskStatus
from anyio import create_task_group, move_on_after


from daffi.utils import colors
from daffi.utils.logger import patch_logger
from daffi.components import ComponentsBase
from daffi.exceptions import GlobalContextError
from daffi.components.proto.message import MessageFlag, messager_pb2, ServiceMessage
from daffi.components.operations.controller_operations import ControllerOperations
from daffi.components.operations.channel_store import ChannelPipe, MessageIterator, FreezableQueue


class Controller(ComponentsBase):

    # ------------------------------------------------------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def controller_callback_mapping(self):
        return self.operations.controller_callback_mapping

    @property
    def channel_store(self):
        return self.operations.channel_store

    @property
    def stream_store(self):
        return self.operations.stream_store

    # ------------------------------------------------------------------------------------------------------------------
    # Controller lifecycle ( on_init -> before_connect -> on_stop )
    # ------------------------------------------------------------------------------------------------------------------

    async def on_init(self) -> NoReturn:
        self.logger = patch_logger(logging.getLogger(self.__class__.__name__.lower()), colors.yellow)
        self.operations = ControllerOperations(logger=self.logger)

    async def on_stop(self) -> NoReturn:
        await super().on_stop()
        self.logger.debug("On stop event triggered")
        self.logger.debug("Wait all channels to be unlocked")

        await self.operations.wait_all_channels_unlocked()
        self.logger.debug("All channels unlocked.")

        async with create_task_group() as sg:
            with move_on_after(2):
                sg.start_soon(FreezableQueue.clear_all)
                await self.operations.wait_all_requests_done()
            with move_on_after(2):
                if self.listener:
                    sg.start_soon(self.listener.stop, 2)
        self.logger.info(f"{self.__class__.__name__} stopped.")
        self._stopped = True

    async def before_connect(self) -> NoReturn:
        await FreezableQueue.clear_all()
        self.listener = None

    # ------------------------------------------------------------------------------------------------------------------
    # Message operations
    # ------------------------------------------------------------------------------------------------------------------

    async def handle_operations(self, task_status: TaskStatus) -> NoReturn:
        self.listener = await self.create_listener()
        if task_status._future._state == "PENDING":
            task_status.started("STARTED")
        self.logger.info(
            f"Controller has been started successfully."
            f" Process identificator: {self.process_name!r}."
            f" Connection info: {self.info}"
        )
        await self.listener.wait_for_termination()

    async def handle_commands(self, channel: ChannelPipe, process_identificator: str):

        async with create_task_group() as sg:
            async for msg in channel:
                if msg.flag in (MessageFlag.HANDSHAKE, MessageFlag.UPDATE_CALLBACKS):
                    await self.operations.on_handshake(msg, channel)

                elif msg.flag == MessageFlag.REQUEST:
                    await self.operations.on_request(msg)

                elif msg.flag == MessageFlag.SUCCESS:
                    await self.operations.on_success(msg)

                elif msg.flag == MessageFlag.SCHEDULER_ERROR:
                    await self.operations.on_scheduler_error(msg)

                elif msg.flag in (MessageFlag.BROADCAST, MessageFlag.STOP_REQUEST):
                    await self.operations.on_broadcast(msg, self.process_name)

                elif msg.flag == MessageFlag.RECK_REQUEST:
                    await self.operations.on_reconnect(msg, sg)

                elif msg.flag == MessageFlag.INIT_STREAM:
                    await self.operations.on_stream_init(msg)

                elif msg.flag == MessageFlag.STREAM_THROTTLE:
                    await self.operations.on_stream_throttle(msg)

            await self.operations.on_channel_close(channel, process_identificator)

    async def communicate(self, request_iterator, context):
        try:
            ident = next(v for k, v in context.invocation_metadata() if k == "ident")
            # Modify ident in order to handle cases when Node and Controller are running in the same process
            ident = f"{ident}-node"
        except StopIteration:
            GlobalContextError("Process name is not provided in metadata.").fire()

        message_iterator = MessageIterator(FreezableQueue.factory(ident))
        channel = ChannelPipe(receive_iterator=request_iterator, send_iterator=message_iterator)

        asyncio.create_task(self.handle_commands(channel, ident))
        async for message in channel.send_iterator:
            yield message

    # ------------------------------------------------------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------------------------------------------------------

    async def stream_to_controller(self, request_iterator, context):
        """Dedicated method for stream transmitter -> controller"""
        throttle_threshold_step = 40
        throttle_time = prev_throttle_time = 0
        receiver = next(v for k, v in context.invocation_metadata() if k == "receiver")
        msg_uuid = next(v for k, v in context.invocation_metadata() if k == "uuid")
        converted_msg_uuid = int(msg_uuid)

        with self.stream_store.get_or_create_stream_pair_cm(receiver, msg_uuid) as stream_pair:
            async for msg in request_iterator:

                q_size = stream_pair.q.size
                if throttle_time and q_size < throttle_threshold_step:
                    throttle_time = 0
                    data = self.operations.awaited_stream_procs.get(converted_msg_uuid)
                    if data:
                        transmitter, _ = data
                        stream_throttle_msg = ServiceMessage(
                            flag=MessageFlag.STREAM_THROTTLE,
                            transmitter=receiver,
                            receiver=transmitter,
                            uuid=converted_msg_uuid,
                            data=throttle_time,
                        )
                        await self.operations.on_stream_throttle(stream_throttle_msg)
                else:
                    throttle_time, zerro_marker = divmod(q_size, throttle_threshold_step)
                    throttle_time /= 15
                    if zerro_marker == 0 and prev_throttle_time != throttle_time:
                        prev_throttle_time = throttle_time
                        data = self.operations.awaited_stream_procs.get(converted_msg_uuid)
                        if data:
                            transmitter, _ = data
                            stream_throttle_msg = ServiceMessage(
                                flag=MessageFlag.STREAM_THROTTLE,
                                transmitter=receiver,
                                receiver=transmitter,
                                uuid=converted_msg_uuid,
                                data=throttle_time,
                            )
                            await self.operations.on_stream_throttle(stream_throttle_msg)

                await stream_pair.send(msg)
            await stream_pair.stop()
        return messager_pb2.Empty()

    async def stream_from_controller(self, request, context):
        """Dedicated method for stream controller -> receiver"""

        receiver = next(v for k, v in context.invocation_metadata() if k == "receiver")
        msg_uuid = next(v for k, v in context.invocation_metadata() if k == "uuid")

        with self.stream_store.get_or_create_stream_pair_cm(receiver, msg_uuid) as stream_pair:
            async for msg in stream_pair.q.iterate():
                yield msg
