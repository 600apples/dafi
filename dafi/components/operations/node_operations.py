import sys
import pickle
import logging
from typing import NoReturn

from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus
from anyio.abc._sockets import SocketStream

from dafi.async_result import AsyncResult
from dafi.message import Message, MessageFlag, RemoteError
from dafi.utils.debug import with_debug_trace
from dafi.components.scheduler import Scheduler
from dafi.utils.misc import run_in_threadpool

from dafi.utils.settings import (
    LOCAL_CALLBACK_MAPPING,
    NODE_CALLBACK_MAPPING,
)
from dafi.components.operations.socket_store import SocketPipe

import tblib.pickling_support

tblib.pickling_support.install()


class NodeOperations:
    """Node operations specification object"""

    def __init__(self, logger: logging.Logger, stream: SocketStream, global_terminate_event):
        self.logger = logger
        AsyncResult.fold_results()
        self.initial_log_shown = False

        self.stream = SocketPipe(stream=stream, global_terminate_event=global_terminate_event)

    @with_debug_trace
    async def on_stream_close(self) -> NoReturn:
        NODE_CALLBACK_MAPPING.clear()
        LOCAL_CALLBACK_MAPPING.clear()
        await self.stream.aclose()
        for msg_uuid, ares in AsyncResult._awaited_results.items():
            AsyncResult._awaited_results[msg_uuid] = None
            if hasattr(ares, "_ready"):
                ares._ready.set()

    @with_debug_trace
    async def on_handshake(self, msg: Message, task_status: TaskStatus, process_name: str, info: str):
        NODE_CALLBACK_MAPPING.clear()
        NODE_CALLBACK_MAPPING.update(msg.func_args[0])

        if task_status._future._state == "PENDING":
            # Consider Node to be started only after handshake response is received.
            task_status.started("STARTED")

        if not self.initial_log_shown:
            self.initial_log_shown = True
            self.logger.info(
                f"Node has been started successfully. Process identificator: {process_name!r}. Connection info: {info}"
            )

    @with_debug_trace
    async def on_request(
        self,
        msg: Message,
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
            await self.stream.send(
                Message(
                    flag=MessageFlag.SUCCESS,
                    transmitter=process_name,
                    receiver=msg.transmitter,
                    uuid=msg.uuid,
                    func_name=msg.func_name,
                    return_result=msg.return_result,
                    error=RemoteError(info=info.replace("locally", "on remote process")),
                )
            )
            self.logger.error(info)

        elif msg.period:
            await scheduler.register(msg=msg, stream=self.stream)

        else:
            sg.start_soon(
                self._remote_func_executor,
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
                self.logger.warning(f"Result {msg.uuid} was taken by timeout")
            elif not ares and error:
                # Result already taken by timeout. No need to raise error but need to notify about
                # finally remote call returned exception.
                error.show_in_log(logger=self.logger)

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
                    error.show_in_log(logger=self.logger)

    @with_debug_trace
    async def on_scheduler_accept(self, msg: Message, process_name: str):
        transmitter = msg.transmitter
        ares = AsyncResult._awaited_results.get(msg.uuid)
        if ares:
            AsyncResult._awaited_results[msg.uuid] = transmitter
            ares._ready.set()
        else:
            self.logger.error(
                f"Unable to find message uuid to accept {msg.func_name!r} scheduler task in process {process_name!r}"
            )

    @with_debug_trace
    async def on_unable_to_find(self, msg):
        if msg.return_result:
            ares = AsyncResult._awaited_results.get(msg.uuid)
            if ares:
                AsyncResult._awaited_results[msg.uuid] = msg.error
                ares._ready.set()
        else:
            self.logger.error(msg.error.info)

    @with_debug_trace
    async def _remote_func_executor(
        self,
        remote_callback: "RemoteCallback",
        message: Message,
        process_name: str,
    ):
        result = error = None
        fn_args = message.func_args
        fn_kwargs = message.func_kwargs

        try:
            if remote_callback.is_async:
                result = await remote_callback(*fn_args, **fn_kwargs)
            else:
                result = await run_in_threadpool(remote_callback, *fn_args, **fn_kwargs)
        except TypeError as e:
            if "were given" in str(e) or "got an unexpected" in str(e) or "missing" in str(e):
                info = f"{e}. Function signature is: {message.func_name}{remote_callback.signature}: ..."
            else:
                info = f"Exception while processing function {message.func_name!r} on remote executor."
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))
        except Exception as e:
            info = f"Exception while processing function {message.func_name!r} on remote executor."
            self.logger.error(info + f" {e}")
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
                )
            except TypeError as e:
                info = f"Unsupported return type {type(result)}."
                self.logger.error(info + f" {e}")
                error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))
                message_to_return = Message(
                    flag=MessageFlag.SUCCESS,
                    transmitter=process_name,
                    receiver=message.transmitter,
                    uuid=message.uuid,
                    func_name=message.func_name,
                    return_result=message.return_result,
                    error=error,
                )

            await self.stream.send(message_to_return)
