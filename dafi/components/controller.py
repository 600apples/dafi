import logging
import asyncio
from functools import partial

from anyio.abc import TaskStatus
from anyio import create_task_group


from dafi.utils import colors
from dafi.utils.logger import patch_logger
from dafi.components import ComponentsBase
from dafi.components.proto.message import MessageFlag
from dafi.components.operations.controller_operations import ControllerOperations
from dafi.components.operations.channel_store import ChannelPipe, MessageIterator


logger = patch_logger(logging.getLogger(__name__), colors.blue)


class Controller(ComponentsBase):
    async def _handle(self, task_status: TaskStatus):

        self.logger = patch_logger(logging.getLogger(__name__), colors.green)

        if not self.operations:
            self.operations = ControllerOperations(logger=logger)

        listener = await self.create_listener()
        self.register_on_stop_callback(partial(listener.stop, grace=True))

        # sg.start_soon(listener.wait_for_termination)
        if task_status._future._state == "PENDING":
            task_status.started("STARTED")
        logger.info(
            f"Controller has been started successfully."
            f" Process identificator: {self.process_name!r}."
            f" Connection info: {self.info}"
        )

    async def handle_commands(self, channel: ChannelPipe):

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

                elif msg.flag == MessageFlag.BROADCAST:
                    await self.operations.on_broadcast(msg, self.process_name)

                elif msg.flag == MessageFlag.RECK_REQUEST:
                    await self.operations.on_reconnect(msg, sg)

            await self.operations.on_channel_close(channel)

    async def communicate(self, request_iterator, context):

        message_iterator = MessageIterator(global_terminate_event=self.global_terminate_event)
        channel = ChannelPipe(receive_iterator=request_iterator, send_iterator=message_iterator)

        asyncio.create_task(self.handle_commands(channel))
        async for message in channel.send_iterator:
            yield message
