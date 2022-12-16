import sys
import logging
import pickle
import asyncio
from asyncio import Queue
from typing import Union, Optional, NoReturn

from anyio import (
    sleep,
    create_task_group,
    connect_unix,
    move_on_after,
    TASK_STATUS_IGNORED,
    EndOfStream,
    BrokenResourceError,
)
from anyio._backends._asyncio import TaskGroup
from anyio.abc import TaskStatus
from anyio.abc._sockets import SocketStream
from tenacity import AsyncRetrying, wait_fixed, retry_if_exception_type

from dafi.utils import colors
from dafi.utils.logger import patch_logger
from dafi.async_result import AsyncResult
from dafi.components import UnixComponentBase
from dafi.message import Message, MessageFlag, RemoteError
from dafi.utils.debug import with_debug_trace

from dafi.exceptions import UnableToFindCandidate, RemoteStoppedUnexpectedly
from dafi.utils.mappings import LOCAL_CALLBACK_MAPPING, NODE_CALLBACK_MAPPING

import tblib.pickling_support

tblib.pickling_support.install()


logger = patch_logger(logging.getLogger(__name__), colors.green)


class Node(UnixComponentBase):
    @with_debug_trace
    async def handle(self, *, task_status: TaskStatus = TASK_STATUS_IGNORED):
        self.loop = asyncio.get_running_loop()
        self.item_store = Queue(loop=self.loop)

        self.operations = NodeOperations()
        async for attempt in AsyncRetrying(
            wait=wait_fixed(3),
            retry=retry_if_exception_type((ConnectionRefusedError, EndOfStream, BrokenResourceError)),
        ):
            if self.stop_event.is_set():
                break

            with attempt:

                if not attempt.retry_state.attempt_number % 5:
                    logger.error("Unable to connect node. retrying...")

                async with await connect_unix(self.socket) as stream:
                    async with create_task_group() as sg:

                        sg.start_soon(self._write_commands, stream, sg)
                        sg.start_soon(self._read_commands, stream, task_status, sg)

    @with_debug_trace
    async def _read_commands(self, stream: SocketStream, task_status: TaskStatus, sg: TaskGroup):
        while not self.stop_event.is_set():
            with move_on_after(1) as scope:
                try:
                    raw_msglen = await stream.receive(4)
                except EndOfStream:
                    await sg.cancel_scope.cancel()
                    raise

            if scope.cancel_called or not raw_msglen:
                continue

            msglen = Message.msglen(raw_msglen)
            msg = Message.loads(await stream.receive(msglen))

            if msg.flag in (MessageFlag.HANDSHAKE, MessageFlag.UPDATE_CALLBACKS):
                await self.operations.on_handshake(msg, task_status, self.process_name)

            elif msg.flag == MessageFlag.REQUEST:
                await self.operations.on_request(msg, stream, sg, self.process_name)

            elif msg.flag == MessageFlag.SUCCESS:
                await self.operations.on_success(msg)

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
        await stream.send(
            Message(
                flag=MessageFlag.HANDSHAKE,
                transmitter=self.process_name,
                func_args=(LOCAL_CALLBACK_MAPPING,),
            ).dumps()
        )
        while not self.stop_event.is_set():
            item, eta = await self.item_store.get()
            sg.start_soon(self._send_item, item, eta, stream, sg)
        await stream.aclose()

    @with_debug_trace
    async def _send_item(self, item: bytes, eta: Union[int, float], stream: SocketStream, sg: TaskGroup):
        await sleep(eta)
        try:
            await stream.send(item)
        except BrokenResourceError:
            await sg.cancel_scope.cancel()

    @with_debug_trace
    def send(self, message: bytes, eta: Union[int, float]):
        asyncio.run_coroutine_threadsafe(self.item_store.put((message, eta)), self.loop).result()

        # self.item_store.put_nowait((message, eta))

    @with_debug_trace
    def register_result(self, result: Optional[AsyncResult]) -> NoReturn:
        if result:
            AsyncResult._awaited_results[result.uuid] = result


class NodeOperations:
    """Node operations specification object"""

    @with_debug_trace
    async def on_handshake(self, msg: Message, task_status: TaskStatus, process_name: str):
        NODE_CALLBACK_MAPPING.clear()
        NODE_CALLBACK_MAPPING.update(msg.func_args[0])
        if task_status._future._state == "PENDING":
            # Consider Node to be started only after handshake response is received.
            task_status.started("STARTED")
            logger.info(f"Node has been started successfully. Process name: {process_name!r}")

    @with_debug_trace
    async def on_request(self, msg: Message, stream: SocketStream, sg: TaskGroup, process_name: str):

        cb_info = LOCAL_CALLBACK_MAPPING.get(msg.func_name)
        if not cb_info:
            info = f"Function {msg.func_name!r} is not registered as callback locally. Make sure python loaded module where callback is located."
            sg.start_soon(
                stream.send,
                Message(
                    flag=MessageFlag.SUCCESS,
                    transmitter=process_name,
                    receiver=msg.transmitter,
                    uuid=msg.uuid,
                    func_name=msg.func_name,
                    return_result=msg.return_result,
                    error=RemoteError(info=info.replace("locally", "on remote process")),
                ).dumps(),
            )
            logger.error(info)

        else:
            sg.start_soon(
                self._remote_func_executor,
                stream,
                cb_info,
                msg,
                process_name,
            )

    @with_debug_trace
    async def on_success(self, msg: Message):
        error = msg.error
        if msg.return_result:
            try:
                ares = AsyncResult._awaited_results[msg.uuid]
            except KeyError:
                if not error:
                    logger.warning(f"Result {msg.uuid} was taken by timeout")
                else:
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
                error.show_in_log(logger=logger)

    @with_debug_trace
    async def on_unable_to_find(self, msg):
        if msg.return_result:
            ares = AsyncResult._awaited_results[msg.uuid]
            AsyncResult._awaited_results[msg.uuid] = msg.error
            ares._ready.set()
        else:
            logger.error(msg.error.info)

    @with_debug_trace
    async def _remote_func_executor(
        self, stream: SocketStream, cb_info: "CallbackInfo", message: Message, process_name: str
    ):
        result = error = None
        args = message.func_args
        kwargs = message.func_kwargs

        try:
            result = cb_info.callback(*args, **kwargs)
            if cb_info.is_async:
                result = await result
        except TypeError as e:
            if "were given" in str(e) or "got an unexpected" in str(e) or "missing" in str(e):
                info = f"{e}. Function signature is: {message.func_name}{cb_info.signature}: ..."
            else:
                info = f"Exception while processing function {message.func_name!r} on remote executor."
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))
        except Exception as e:
            info = f"Exception while processing function {message.func_name!r} on remote executor."
            logger.error(info + f" {e}")
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))

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

        await stream.send(message_to_return)
