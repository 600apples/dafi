import logging
import asyncio
from asyncio import Queue
from typing import Union, Optional, NoReturn

from anyio import create_task_group, TASK_STATUS_IGNORED, EndOfStream, maybe_async
from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus

from dafi.utils import colors
from dafi.utils.logger import patch_logger
from dafi.async_result import AsyncResult
from dafi.components import ComponentsBase
from dafi.message import Message, MessageFlag
from dafi.utils.debug import with_debug_trace
from dafi.utils.retry import stoppable_retry, RetryInfo
from dafi.components.scheduler import Scheduler

from dafi.exceptions import UnableToFindCandidate, RemoteStoppedUnexpectedly
from dafi.utils.settings import LOCAL_CALLBACK_MAPPING, WELL_KNOWN_CALLBACKS
from dafi.components.operations.node_operations import NodeOperations


logger = patch_logger(logging.getLogger(__name__), colors.green)


class Node(ComponentsBase):
    @stoppable_retry(wait=2)
    async def handle(
        self,
        global_terminate_event,
        retry_info: RetryInfo,
        task_status: TaskStatus = TASK_STATUS_IGNORED,
    ):
        await super().handle(global_terminate_event)
        self.loop = asyncio.get_running_loop()
        self.item_store = Queue()
        self.global_terminate_event = global_terminate_event

        if self.global_terminate_event.is_set():
            return

        if retry_info.attempt == 3 or not retry_info.attempt % 6:
            logger.error(f"Unable to connect node {self.process_name}. Error = {retry_info.prev_error}. Retrying...")

        async with self.connect_listener() as stream:

            self.operations = NodeOperations(
                logger=logger, stream=stream, global_terminate_event=global_terminate_event
            )

            async with create_task_group() as sg:
                self.scheduler = Scheduler(process_name=self.process_name, sg=sg)
                self.register_on_stop_callback(self.on_node_stop)
                self.register_on_stop_callback(self.scheduler.on_scheduler_stop)

                sg.start_soon(self._write_commands)
                sg.start_soon(self._read_commands, task_status, sg)

    @with_debug_trace
    async def on_node_stop(self):
        await self.operations.on_stream_close()

    @with_debug_trace
    async def _read_commands(self, task_status: TaskStatus, sg: TaskGroup):

        self.handler_counter += 1
        async for msg in self.operations.stream:

            if not msg:
                break

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
                if msg.flag == MessageFlag.REMOTE_STOPPED_UNEXPECTEDLY:
                    msg.error._awaited_error_type = RemoteStoppedUnexpectedly
                else:
                    msg.error._awaited_error_type = UnableToFindCandidate
                await self.operations.on_unable_to_find(msg)

        await self.operations.on_stream_close()
        self.handler_counter -= 1
        await maybe_async(sg.cancel_scope.cancel())
        if not self.global_terminate_event.is_set():
            raise EndOfStream()

    @with_debug_trace
    async def _write_commands(self):
        local_mapping_without_well_known_callbacks = {
            k: v for k, v in LOCAL_CALLBACK_MAPPING.items() if k not in WELL_KNOWN_CALLBACKS
        }
        await self.operations.stream.send(
            Message(
                flag=MessageFlag.HANDSHAKE,
                transmitter=self.process_name,
                func_args=(local_mapping_without_well_known_callbacks,),
            )
        )
        while not self.global_terminate_event.is_set():
            message, eta = await self.item_store.get()
            await self.operations.stream.send(message, eta)

    @with_debug_trace
    def send_threadsave(self, message: bytes, eta: Union[int, float]):
        asyncio.run_coroutine_threadsafe(self.item_store.put((message, eta)), self.loop).result()

    def send(self, message: Message, eta: Union[int, float]):
        self.item_store.put_nowait((message, eta))

    @with_debug_trace
    def register_result(self, result: Optional[AsyncResult]) -> NoReturn:
        if result:
            AsyncResult._awaited_results[result.uuid] = result
