import sys
import logging
import pickle
import asyncio
from asyncio import Queue
from typing import Union, Optional, NoReturn

from anyio import (
    sleep,
    Event,
    maybe_async,
    to_thread,
    create_task_group,
    TASK_STATUS_IGNORED,
)
from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus
from anyio.abc._sockets import SocketStream

from dafi.utils import colors
from dafi.utils.logger import patch_logger
from dafi.async_result import AsyncResult
from dafi.components import ComponentsBase, send_to_stream
from dafi.message import Message, MessageFlag, RemoteError
from dafi.utils.debug import with_debug_trace
from dafi.utils.retry import stoppable_retry, RetryInfo
from dafi.components.scheduler import Scheduler
from dafi.utils.misc import run_in_threadpool

from dafi.exceptions import UnableToFindCandidate, RemoteStoppedUnexpectedly
from dafi.utils.mappings import (
    LOCAL_CALLBACK_MAPPING,
    NODE_CALLBACK_MAPPING,
    WELL_KNOWN_CALLBACKS,
    search_remote_callback_in_mapping,
)

import tblib.pickling_support

tblib.pickling_support.install()


logger = patch_logger(logging.getLogger(__name__), colors.green)


class Node(ComponentsBase):
    @stoppable_retry(wait=3)
    async def handle(
        self,
        global_event,
        retry_info: RetryInfo,
        task_status: TaskStatus = TASK_STATUS_IGNORED,
    ):
        self.global_event = global_event
        self.stop_event = Event()
        self.loop = asyncio.get_running_loop()
        self.item_store = Queue()
        self.operations = NodeOperations(self.stop_event)

        if global_event.is_set():
            return

        if retry_info.attempt == 2 or not retry_info.attempt % 5:
            logger.error(f"Unable to connect node. Error = {retry_info.prev_error}. Retrying...")

        async with self.connect_listener() as stream:
            async with create_task_group() as sg:
                self.scheduler = Scheduler(process_name=self.process_name, sg=sg, global_event=global_event)

                sg.start_soon(self._write_commands, stream, sg)
                sg.start_soon(self._read_commands, stream, task_status, sg)
                sg.start_soon(self.global_event_observer, global_event)

    @with_debug_trace
    async def global_event_observer(self, global_event) -> NoReturn:
        await to_thread.run_sync(global_event.wait)
        LOCAL_CALLBACK_MAPPING.clear()
        NODE_CALLBACK_MAPPING.clear()
        for msg_uuid, ares in AsyncResult._awaited_results.items():
            AsyncResult._awaited_results[msg_uuid] = None
            if hasattr(ares, "_ready"):
                ares._ready.set()

    @with_debug_trace
    async def _read_commands(self, stream: SocketStream, task_status: TaskStatus, sg: TaskGroup):
        while True:
            if self.stop_event.is_set():
                raise ConnectionResetError("Node disconnected from controller.")
            try:
                raw_msglen = await stream.receive(4)
                if not raw_msglen:
                    continue

                msg = await Message.loads(stream, raw_msglen)
                if not msg:
                    continue
            except Exception:
                await maybe_async(sg.cancel_scope.cancel())
                if self.global_event.is_set():
                    return
                raise

            if msg.flag in (MessageFlag.HANDSHAKE, MessageFlag.UPDATE_CALLBACKS):
                await self.operations.on_handshake(msg, task_status, self.process_name, self.info)

            elif msg.flag in (MessageFlag.REQUEST, MessageFlag.BROADCAST):
                await self.operations.on_request(msg, stream, sg, self.process_name, self.scheduler)

            elif msg.flag == MessageFlag.SUCCESS:
                await self.operations.on_success(msg, self.scheduler)

            elif msg.flag == MessageFlag.SCHEDULER_ACCEPT:
                await self.operations.on_scheduler_accept(msg)

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

    @with_debug_trace
    async def _write_commands(self, stream: SocketStream, sg: TaskGroup):
        await send_to_stream(
            stream,
            Message(
                flag=MessageFlag.HANDSHAKE,
                transmitter=self.process_name,
                func_args=(LOCAL_CALLBACK_MAPPING,),
            ).dumps(),
            self.stop_event,
        )
        while not self.stop_event.is_set():
            item, eta = await self.item_store.get()
            sg.start_soon(self._send_item, item, eta, stream)

    @with_debug_trace
    async def _send_item(self, item: bytes, eta: Union[int, float], stream: SocketStream):
        await sleep(eta)
        await send_to_stream(stream, item, self.stop_event)

    @with_debug_trace
    def send_threadsave(self, message: bytes, eta: Union[int, float]):
        asyncio.run_coroutine_threadsafe(self.item_store.put((message, eta)), self.loop).result()

    def send(self, message: bytes, eta: Union[int, float]):
        self.item_store.put_nowait((message, eta))

    @with_debug_trace
    def register_result(self, result: Optional[AsyncResult]) -> NoReturn:
        if result:
            AsyncResult._awaited_results[result.uuid] = result


class NodeOperations:
    """Node operations specification object"""

    def __init__(self, stop_event: Event):
        self.stop_event = stop_event
        AsyncResult.fold_results()
        self.initial_log_shown = False

    @with_debug_trace
    async def on_handshake(self, msg: Message, task_status: TaskStatus, process_name: str, info: str):
        NODE_CALLBACK_MAPPING.clear()
        NODE_CALLBACK_MAPPING.update(msg.func_args[0])
        if task_status._future._state == "PENDING":
            # Consider Node to be started only after handshake response is received.
            task_status.started("STARTED")

        if not self.initial_log_shown:
            self.initial_log_shown = True
            logger.info(f"Node has been started successfully. Process name: {process_name!r}. Connection info: {info}")

        for func_name in set(LOCAL_CALLBACK_MAPPING).difference(WELL_KNOWN_CALLBACKS):
            data = search_remote_callback_in_mapping(
                func_name=func_name, exclude=process_name, mapping=NODE_CALLBACK_MAPPING
            )
            if data:
                proc, _ = data
                logger.warning(
                    f"A remote callback named {func_name!r} is already registered in the {proc!r} process."
                    f" 2 callbacks with the same name can lead to undesirable consequences, since"
                    f" when calling a callback from a remote process, only one of them will be executed"
                )

    @with_debug_trace
    async def on_request(
        self,
        msg: Message,
        stream: SocketStream,
        sg: TaskGroup,
        process_name: str,
        scheduler: Scheduler,
    ):

        remote_callback = LOCAL_CALLBACK_MAPPING.get(msg.func_name)
        if not remote_callback:
            info = (
                f"Function {msg.func_name!r} is not registered as callback locally."
                f" Make sure python loaded module where callback is located."
            )
            sg.start_soon(
                send_to_stream,
                stream,
                Message(
                    flag=MessageFlag.SUCCESS,
                    transmitter=process_name,
                    receiver=msg.transmitter,
                    uuid=msg.uuid,
                    func_name=msg.func_name,
                    return_result=msg.return_result,
                    error=RemoteError(info=info.replace("locally", "on remote process")),
                ).dumps(),
                self.stop_event,
            )
            logger.error(info)

        elif msg.period:
            await scheduler.register(msg=msg, stream=stream)

        else:
            sg.start_soon(
                self._remote_func_executor,
                stream,
                remote_callback,
                msg,
                process_name,
            )

    @with_debug_trace
    async def on_success(self, msg: Message, scheduler: Scheduler):
        error = msg.error

        if msg.return_result:
            ares = AsyncResult._awaited_results.get(msg.uuid)
            if not ares and not error:
                logger.warning(f"Result {msg.uuid} was taken by timeout")
            elif not ares and error:
                # Result already taken by timeout. No need to raise error but need to notify about
                # finally remote call returned exception.
                error.show_in_log(logger=logger)

            else:
                if error:
                    AsyncResult._awaited_results[msg.uuid] = error
                else:
                    AsyncResult._awaited_results[msg.uuid] = msg.func_args[0]
                ares._ready.set()
        else:
            if error:
                if msg.period:
                    await scheduler.on_error(msg)
                else:
                    error.show_in_log(logger=logger)

    @with_debug_trace
    async def on_scheduler_accept(self, msg: Message):
        transmitter = msg.transmitter
        ares = AsyncResult._awaited_results.get(msg.uuid)
        if ares:
            AsyncResult._awaited_results[msg.uuid] = transmitter
            ares._ready.set()
        else:
            logger.error(f"Unable to find message uuid to accept {msg.func_name!r} scheduler task")

    @with_debug_trace
    async def on_unable_to_find(self, msg):
        if msg.return_result:
            ares = AsyncResult._awaited_results.get(msg.uuid)
            if ares:
                AsyncResult._awaited_results[msg.uuid] = msg.error
                ares._ready.set()
        else:
            logger.error(msg.error.info)

    @with_debug_trace
    async def _remote_func_executor(
        self,
        stream: SocketStream,
        remote_callback: "RemoteCallback",
        message: Message,
        process_name: str,
    ):
        result = error = None
        args = message.func_args
        kwargs = message.func_kwargs

        try:
            if remote_callback.is_async:
                result = await remote_callback(*args, **kwargs)
            else:
                result = await run_in_threadpool(remote_callback, *args, **kwargs)
        except TypeError as e:
            if "were given" in str(e) or "got an unexpected" in str(e) or "missing" in str(e):
                info = f"{e}. Function signature is: {message.func_name}{remote_callback.signature}: ..."
            else:
                info = f"Exception while processing function {message.func_name!r} on remote executor."
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))
        except Exception as e:
            info = f"Exception while processing function {message.func_name!r} on remote executor."
            logger.error(info + f" {e}")
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))

        if message.flag != MessageFlag.BROADCAST:
            try:
                message_to_return = Message(
                    flag=MessageFlag.SUCCESS,
                    transmitter=process_name,
                    receiver=message.transmitter,
                    uuid=message.uuid,
                    func_name=message.func_name,
                    func_args=(result,) if (message.return_result and not error) else None,
                    return_result=message.return_result,
                    error=error,
                ).dumps()
            except TypeError as e:
                info = f"Unsupported return type {type(result)}."
                logger.error(info + f" {e}")
                error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))
                message_to_return = Message(
                    flag=MessageFlag.SUCCESS,
                    transmitter=process_name,
                    receiver=message.transmitter,
                    uuid=message.uuid,
                    func_name=message.func_name,
                    return_result=message.return_result,
                    error=error,
                ).dumps()

            await send_to_stream(stream, message_to_return, self.stop_event)
