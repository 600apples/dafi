import sys
import pickle
import logging
from typing import NoReturn, Dict, Optional

from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus, CancelScope

from daffi.async_result import AsyncResult
from daffi.utils.custom_types import GlobalCallback, K
from daffi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag, RemoteError
from daffi.utils.debug import with_debug_trace
from daffi.components.scheduler import Scheduler
from daffi.utils.misc import run_in_threadpool, run_from_working_thread

from daffi.utils.settings import (
    LOCAL_CALLBACK_MAPPING,
)
from daffi.components.operations.channel_store import ChannelPipe
from daffi.exceptions import ReckAcceptError, StopComponentError

import tblib.pickling_support

tblib.pickling_support.install()


class NodeOperations:
    """Node operations specification object"""

    def __init__(self, logger: logging.Logger, async_backend: str):
        self.logger = logger
        self._channel = None
        self.async_backend = async_backend
        self.node_callback_mapping: Dict[K, Dict[K, GlobalCallback]] = dict()

    @property
    def channel(self) -> ChannelPipe:
        return self._channel

    @channel.setter
    def channel(self, channel: ChannelPipe) -> NoReturn:
        self._channel = channel
        self._channel.proceed()

    @with_debug_trace
    async def on_handshake(self, msg: ServiceMessage, task_status: TaskStatus, process_name: str, info: str):
        msg.loads()
        self.node_callback_mapping = msg.data

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
            with CancelScope(shield=True):
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
                result = error if error else msg.func_args[0]
                AsyncResult._set_and_trigger(msg.uuid, result)
        else:
            if error:
                if msg.period:
                    await scheduler.on_error(msg)
                else:
                    error.show_in_log(logger=self.logger)

    @with_debug_trace
    async def on_scheduler_accept(self, msg: RpcMessage, process_name: str):
        transmitter = msg.transmitter
        if AsyncResult._awaited_results.get(msg.uuid):
            AsyncResult._set_and_trigger(msg.uuid, transmitter)
        else:
            self.logger.error(
                f"Unable to find message uuid to accept {msg.func_name!r} scheduler task in process {process_name!r}"
            )

    @with_debug_trace
    async def on_unable_to_find(self, msg: RpcMessage):
        msg.loads()
        if msg.return_result:
            if AsyncResult._awaited_results.get(msg.uuid):
                AsyncResult._set_and_trigger(msg.uuid, msg.error)
        else:
            self.logger.error(msg.error.info)

    async def on_reconnection(self):
        self.channel.freeze()
        raise ReckAcceptError()

    async def on_stop_request(self, msg: RpcMessage, process_name: str):
        self.logger.info(f"Termination signal received.")
        message_to_return = RpcMessage(
            flag=MessageFlag.SUCCESS,
            transmitter=process_name,
            receiver=msg.transmitter,
            uuid=msg.uuid,
            func_name=msg.func_name,
            return_result=True,
            func_args=(None,),
        )
        self.channel.send(message_to_return)
        raise StopComponentError()

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
                result = await run_from_working_thread(self.async_backend, remote_callback, *fn_args, **fn_kwargs)
            else:
                result = await run_in_threadpool(remote_callback, *fn_args, **fn_kwargs)
        except TypeError as e:
            if "were given" in str(e) or "got an unexpected" in str(e) or "missing" in str(e):
                info = f"{e}. Function signature is: {message.func_name}{remote_callback.signature}: ..."
            else:
                info = f"Exception while processing function {message.func_name!r} on remote executor. {e}"
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))
        except Exception as e:
            info = f"Exception while processing function {message.func_name!r} on remote executor. {e}"
            self.logger.error(info)
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
