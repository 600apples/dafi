import sys
import pickle
import logging
from typing import NoReturn

from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus

from dafi.async_result import AsyncResult
from dafi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag, RemoteError
from dafi.utils.debug import with_debug_trace
from dafi.components.scheduler import Scheduler
from dafi.utils.misc import run_in_threadpool

from dafi.utils.settings import (
    LOCAL_CALLBACK_MAPPING,
    NODE_CALLBACK_MAPPING,
)
from dafi.components.operations.channel_store import ChannelPipe

import tblib.pickling_support

tblib.pickling_support.install()


class NodeOperations:
    """Node operations specification object"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        AsyncResult.fold_results()
        self.channel = None

    def set_channel(self, channel: ChannelPipe):
        self.channel = channel

    @with_debug_trace
    async def on_channel_close(self) -> NoReturn:

        self.channel.send_iterator.stop()
        NODE_CALLBACK_MAPPING.clear()
        for msg_uuid, ares in AsyncResult._awaited_results.items():
            AsyncResult._awaited_results[msg_uuid] = None
            if hasattr(ares, "_ready"):
                ares._ready.set()

    @with_debug_trace
    async def on_handshake(self, msg: ServiceMessage, task_status: TaskStatus, process_name: str, info: str):
        msg.loads()
        NODE_CALLBACK_MAPPING.clear()
        NODE_CALLBACK_MAPPING.update(msg.data)

        if task_status._future._state == "PENDING":
            # Consider Node to be started only after handshake response is received.
            task_status.started("STARTED")

        if msg.transmitter == process_name:
            self.logger.info(
                f"Node has been started successfully. Process identificator: {process_name!r}. Connection info: {info}"
            )

    @with_debug_trace
    async def on_request(
        self,
        msg: RpcMessage,
        sg: TaskGroup,
        process_name: str,
        scheduler: Scheduler,
    ):
        msg.loads()
        remote_callback = LOCAL_CALLBACK_MAPPING.get(msg.func_name)
        if not remote_callback:
            info = (
                f"Function {msg.func_name!r} is not registered as callback locally."
                f" Make sure python loaded module where callback is located."
            )
            self.channel.send(
                RpcMessage(
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
            await scheduler.register(msg=msg, channel=self.channel)

        else:
            sg.start_soon(
                self._remote_func_executor,
                remote_callback,
                msg,
                process_name,
            )

    @with_debug_trace
    async def on_success(self, msg: RpcMessage, scheduler: Scheduler):
        msg.loads()
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
    async def on_scheduler_accept(self, msg: RpcMessage, process_name: str):
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
    async def on_unable_to_find(self, msg: RpcMessage):
        msg.loads()
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
        message: RpcMessage,
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

        if message.return_result or message.flag != MessageFlag.BROADCAST:
            try:
                message_to_return = RpcMessage(
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
                message_to_return = RpcMessage(
                    flag=MessageFlag.SUCCESS,
                    transmitter=process_name,
                    receiver=message.transmitter,
                    uuid=message.uuid,
                    func_name=message.func_name,
                    return_result=message.return_result,
                    error=error,
                )
            self.channel.send(message_to_return)
