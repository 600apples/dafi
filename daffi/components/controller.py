import logging
import asyncio
from typing import NoReturn

from anyio.abc import TaskStatus
from anyio import create_task_group, move_on_after


from daffi.utils import colors
from daffi.utils.logger import patch_logger
from daffi.components import ComponentsBase
from daffi.components.proto.message import MessageFlag
from daffi.components.operations.controller_operations import ControllerOperations
from daffi.components.operations.channel_store import ChannelPipe, MessageIterator, FreezableQueue


class Controller(ComponentsBase):

    # ------------------------------------------------------------------------------------------------------------------
    # Controller lifecycle ( on_init -> before_connect -> on_stop )
    # ------------------------------------------------------------------------------------------------------------------

    async def on_init(self) -> NoReturn:
        self.logger = patch_logger(logging.getLogger(__name__), colors.blue)
        self.operations = ControllerOperations(logger=self.logger)

    async def on_stop(self) -> NoReturn:

        async with create_task_group() as sg:
            with move_on_after(2):
                sg.start_soon(FreezableQueue.clear_all)
                await self.operations.wait_all_requests_done()
            with move_on_after(2):
                if self.listener:
                    sg.start_soon(self.listener.stop, 2)
        await super().on_stop()

    async def before_connect(self) -> NoReturn:
        self.listener = None

    # ------------------------------------------------------------------------------------------------------------------

    @property
    def controller_callback_mapping(self):
        return self.operations.controller_callback_mapping

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

            await self.operations.on_channel_close(channel, process_identificator)

    async def communicate(self, request_iterator, context):
        try:
            ident = next(v for k, v in context.invocation_metadata() if k == "ident")
            # Modify ident in order to handle cases when Node and Controller are running in the same process
            ident = f"{ident}-node"
        except StopIteration:
            raise KeyError("Process name is not provided in metadata.")

        message_iterator = MessageIterator(FreezableQueue.factory(ident))
        channel = ChannelPipe(receive_iterator=request_iterator, send_iterator=message_iterator)

        asyncio.create_task(self.handle_commands(channel, ident))
        async for message in channel.send_iterator:
            yield message
