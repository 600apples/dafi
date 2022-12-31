import logging
import time
from typing import NoReturn
from anyio import create_task_group, sleep, TASK_STATUS_IGNORED, ExceptionGroup, ClosedResourceError
from anyio.abc._sockets import SocketStream

from dafi.utils import colors
from dafi.utils.logger import patch_logger
from dafi.components import ComponentsBase
from dafi.message import Message, MessageFlag
from dafi.utils.debug import with_debug_trace
from dafi.utils.retry import stoppable_retry, RetryInfo
from dafi.backend import BackEndKeys, ControllerStatus
from dafi.components.operations.socket_store import SocketPipe
from dafi.components.operations.controller_operations import ControllerOperations


logger = patch_logger(logging.getLogger(__name__), colors.blue)


class Controller(ComponentsBase):
    @stoppable_retry(wait=2, not_acceptable=(ClosedResourceError, ExceptionGroup))
    async def handle(
        self,
        global_terminate_event,
        retry_info: RetryInfo,
        task_status=TASK_STATUS_IGNORED,
    ):
        await super().handle(global_terminate_event)
        self.register_on_stop_callback(self.on_controller_stop)

        self.listener = None
        self.operations = ControllerOperations(logger=logger)
        self.global_terminate_event = global_terminate_event

        if self.global_terminate_event.is_set():
            return

        if retry_info.attempt == 3 or not retry_info.attempt % 6:
            logger.error(f"Unable to connect controller. Error = {retry_info.prev_error}. Retrying...")

        self.listener = await self.create_listener()
        if task_status._future._state == "PENDING":
            task_status.started("STARTED")

        async with create_task_group() as sg:
            sg.start_soon(self.healthcheck)
            sg.start_soon(self.listener.serve, self._handle_commands, sg)
            logger.info(f"Controller has been started successfully. Process identificator: {self.process_name!r}")

    @with_debug_trace
    async def healthcheck(self) -> NoReturn:
        while True:
            self.backend.write(BackEndKeys.CONTROLLER_HEALTHCHECK_TS, time.time())
            await sleep(self.TIMEOUT / 3)

    @with_debug_trace
    async def on_controller_stop(self) -> NoReturn:
        self.backend.delete_key(BackEndKeys.CONTROLLER_HEALTHCHECK_TS)
        self.backend.write(BackEndKeys.CONTROLLER_STATUS, ControllerStatus.UNAVAILABLE)
        await self.operations.close_sockets()
        if self.listener:
            await self.listener.aclose()

    @with_debug_trace
    async def _handle_commands(self, stream: SocketStream):
        self.handler_counter += 1
        stream = SocketPipe(stream=stream, global_terminate_event=self.global_terminate_event)

        async for msg in stream:
            if not msg:
                break

            if msg.flag == MessageFlag.HANDSHAKE:
                await self.operations.on_handshake(msg, stream, self.global_terminate_event)

            elif msg.flag == MessageFlag.UPDATE_CALLBACKS:
                await self.operations.on_update_callbacks(msg)

            elif msg.flag == MessageFlag.REQUEST:
                await self.operations.on_request(msg, self.global_terminate_event)

            elif msg.flag == MessageFlag.SUCCESS:
                await self.operations.on_success(msg)

            elif msg.flag == MessageFlag.SCHEDULER_ERROR:
                await self.operations.on_scheduler_error(msg)

            elif msg.flag == MessageFlag.BROADCAST:
                await self.operations.on_broadcast(msg, self.process_name)

        await self.operations.on_stream_close(stream)
        self.handler_counter -= 1
