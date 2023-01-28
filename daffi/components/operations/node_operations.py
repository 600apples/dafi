import sys
import pickle
import logging
import asyncio
from typing import NoReturn, Dict
from queue import Queue as thQueue

from anyio import run, create_task_group
from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus, CancelScope
from grpc.aio._call import AioRpcError

from daffi.async_result import AsyncResult
from daffi.utils.custom_types import GlobalCallback, K
from daffi.components.proto.message import RpcMessage, ServiceMessage, MessageFlag, RemoteError, messager_pb2, Message

from daffi.components.scheduler import Scheduler
from daffi.utils.misc import run_in_threadpool, run_from_working_thread

from daffi.utils.settings import (
    LOCAL_CALLBACK_MAPPING,
)
from daffi.components.operations.freezable_queue import ItemPriority
from daffi.components.operations.channel_store import ChannelPipe
from daffi.exceptions import ReckAcceptError, StopComponentError
from daffi.components.operations.streams_store import StreamPairStore
from daffi.utils.misc import ReconnectFreq

import tblib.pickling_support

tblib.pickling_support.install()


class NodeOperations:
    """Node operations specification object"""

    def __init__(self, logger: logging.Logger, async_backend: str, reconnect_freq: ReconnectFreq):
        self.logger = logger
        self._channel = None
        self.stream_store = StreamPairStore()
        self.async_backend = async_backend
        self.reconnect_freq = reconnect_freq
        self.node_callback_mapping: Dict[K, Dict[K, GlobalCallback]] = dict()

    @property
    def channel(self) -> ChannelPipe:
        return self._channel

    @channel.setter
    def channel(self, channel: ChannelPipe) -> NoReturn:
        self._channel = channel
        self._channel.proceed()

    async def on_handshake(self, msg: ServiceMessage, task_status: TaskStatus, process_name: str, info: str):
        try:
            msg.loads()
        except Exception as e:
            if msg.transmitter == process_name:
                self.logger.error(f"{e}")
                raise StopComponentError()
        else:
            self.reconnect_freq.unlock()
            self.node_callback_mapping = msg.data
            if msg.transmitter == process_name and msg.flag == MessageFlag.HANDSHAKE:
                self.logger.info(
                    f"Node has been started successfully. Process identificator: {process_name!r}. Connection info: {info}"
                )
                if task_status._future._state == "PENDING":
                    # Consider Node to be started only after handshake response is received.
                    task_status.started("STARTED")

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
            await self.channel.send(
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
            with self.reconnect_freq.locker():
                await scheduler.register(msg=msg, channel=self.channel)

        else:
            with CancelScope(shield=True):
                sg.start_soon(
                    self._remote_func_executor,
                    remote_callback,
                    msg,
                    process_name,
                )

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
        # Close stream if not closed
        stream_pair = self.stream_store.get(f"{msg.transmitter}-{msg.uuid}")
        if stream_pair:
            stream_pair.closed = True
            await stream_pair.stop(ItemPriority.FIRST)

    async def on_scheduler_accept(self, msg: RpcMessage, process_name: str):
        transmitter = msg.transmitter
        if AsyncResult._awaited_results.get(msg.uuid):
            AsyncResult._set_and_trigger(msg.uuid, transmitter)
        else:
            self.logger.error(
                f"Unable to find message uuid to accept {msg.func_name!r} scheduler task in process {process_name!r}"
            )

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
        await self.channel.send(message_to_return)
        raise StopComponentError()

    async def on_stream_init(self, msg: RpcMessage, stub, sg, process_name):
        msg.loads()
        remote_callback = LOCAL_CALLBACK_MAPPING.get(msg.func_name)
        if not remote_callback:
            info = (
                f"Function {msg.func_name!r} is not registered as callback locally."
                f" Make sure python loaded module where callback is located."
            )
            self.logger.error(info)

        else:
            with CancelScope(shield=True):
                sg.start_soon(
                    self._remote_func_stream_executor,
                    remote_callback,
                    msg,
                    process_name,
                    stub,
                )

    async def on_stream_error(self, msg: RpcMessage):
        msg.loads()
        error = msg.error
        ares = AsyncResult._awaited_results.get(msg.uuid)
        if not ares:
            self.logger.warning(f"Result {msg.uuid} was taken by timeout")
        elif not ares and error:
            # Result already taken by timeout. No need to raise error but need to notify about
            # finally remote call returned exception.
            error.show_in_log(logger=self.logger)
        AsyncResult._set_and_trigger(msg.uuid, error)

        stream_pair = self.stream_store.get(f"{msg.transmitter}-{msg.uuid}")
        if stream_pair:
            stream_pair.closed = True
            await stream_pair.stop(ItemPriority.FIRST)

    async def on_stream_throttle(self, msg: ServiceMessage):
        msg.loads()
        stream_pair_group = self.stream_store.stream_pair_group_store.get(str(msg.uuid))
        if stream_pair_group:
            stream_pair_group.throttle_time = msg.data

    async def _remote_func_stream_executor(
        self,
        remote_callback: "RemoteCallback",
        message: RpcMessage,
        process_name: str,
        stub,
    ):
        message_iterator = Message.from_message_iterator(
            stub.stream_from_controller(
                messager_pb2.Empty(), metadata=[("receiver", message.receiver), ("uuid", str(message.uuid))]
            )
        )
        error = None
        items_queue = thQueue()
        _stop_marker = object()

        def _stream_executor():
            async def _process_stream():
                while True:
                    item = items_queue.get()
                    if item is _stop_marker:
                        break

                    res = remote_callback(item)
                    if remote_callback.is_async:
                        await res

            run(_process_stream, backend=self.async_backend)

        async def _stream_poller():
            throttle_threshold_step = 3
            throttle_time = prev_throttle_time = 0

            try:
                async for msg in message_iterator:
                    msg.loads()
                    items_queue.put_nowait(msg.data)

                    q_size = items_queue.qsize()
                    if throttle_time and q_size < throttle_threshold_step:
                        throttle_time = 0
                        msg = ServiceMessage(
                            flag=MessageFlag.STREAM_THROTTLE,
                            transmitter=process_name,
                            receiver=message.transmitter,
                            uuid=message.uuid,
                            data=throttle_time,
                        )
                        await self.channel.send(msg)

                    else:
                        throttle_time, zerro_marker = divmod(q_size, throttle_threshold_step)
                        throttle_time /= 5
                        if zerro_marker == 0 and prev_throttle_time != throttle_time:
                            prev_throttle_time = throttle_time
                            msg = ServiceMessage(
                                flag=MessageFlag.STREAM_THROTTLE,
                                transmitter=process_name,
                                receiver=message.transmitter,
                                uuid=message.uuid,
                                data=throttle_time,
                            )
                            await self.channel.send(msg)

            except AioRpcError:
                with items_queue.mutex:
                    items_queue.queue.clear()
            finally:
                items_queue.put_nowait(_stop_marker)

        with self.reconnect_freq.locker():
            stream_poller = asyncio.create_task(_stream_poller())
            try:
                await run_in_threadpool(_stream_executor)
                self.logger.debug(f"Stop streaming process for function: {message.func_name}. Process = {process_name}")
            except Exception as e:
                err_msg = f"Exception while streaming to function {message.func_name!r} on remote executor {process_name!r}. {e}"
                error = RemoteError(info=err_msg, traceback=pickle.dumps(sys.exc_info()))
                self.logger.error(err_msg)
            finally:
                stream_poller.cancel()

            message_to_return = RpcMessage(
                flag=MessageFlag.SUCCESS,
                transmitter=process_name,
                receiver=message.transmitter,
                uuid=message.uuid,
                func_name=message.func_name,
                func_args=(None,),
                error=error,
            )
            await self.channel.send(message_to_return)

    async def _remote_func_executor(
        self,
        remote_callback: "RemoteCallback",
        message: RpcMessage,
        process_name: str,
    ):
        with self.reconnect_freq.locker():
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
                await self.channel.send(message_to_return)
