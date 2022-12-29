import sys
import time
import pickle
import logging
import asyncio
from dataclasses import dataclass
from threading import Event as thEvent

from anyio import sleep
from anyio._backends._asyncio import TaskGroup
from anyio.abc._sockets import SocketStream

from dafi.utils import colors
from dafi.message import Message, RemoteError, MessageFlag
from dafi.utils.logger import patch_logger
from dafi.components import send_to_stream
from dafi.utils.misc import run_in_threadpool
from dafi.utils.debug import with_debug_trace
from dafi.utils.mappings import LOCAL_CALLBACK_MAPPING, SCHEDULER_PERIODICAL_TASKS, SCHEDULER_AT_TIME_TASKS


logger = patch_logger(logging.getLogger(__name__), colors.magenta)


@dataclass
class Scheduler:
    sg: TaskGroup
    process_name: str
    stop_event: thEvent

    @with_debug_trace
    async def register(self, msg: Message, stream: SocketStream):
        if msg.period.period:
            if msg.func_name in SCHEDULER_PERIODICAL_TASKS:
                SCHEDULER_PERIODICAL_TASKS[msg.func_name].cancel()
            SCHEDULER_PERIODICAL_TASKS[msg.func_name] = asyncio.create_task(
                self.on_period(msg.period.period, stream, msg)
            )

        elif msg.period.at_time:
            SCHEDULER_AT_TIME_TASKS[msg.uuid] = [
                asyncio.create_task(self.on_at_time(ts, stream, msg)) for ts in msg.period.at_time
            ]

    @with_debug_trace
    async def on_error(self, msg: Message):
        msg.error.show_in_log(logger=logger)

    @with_debug_trace
    async def on_period(self, period: int, stream: SocketStream, msg: Message):
        while not self.stop_event.is_set():
            await sleep(period)
            if not await self._remote_func_executor(stream, msg, "period"):
                logger.error(
                    f"Callback {msg.func_name} cannot be executed periodically"
                    f" since unrecoverable error detected. Stop scheduling..."
                )

    @with_debug_trace
    async def on_at_time(self, ts: int, stream: SocketStream, msg: Message):
        now = time.time()
        delta = ts - now
        if delta < 0:
            info = (
                f"Scheduler unable to execute callback {msg.func_name} "
                f"at timestamp {ts} since this timestamp already in the past."
            )
            logger.error(info)
            error = RemoteError(info=info)
            try:
                await send_to_stream(
                    stream,
                    Message(
                        flag=MessageFlag.SCHEDULER_ERROR,
                        transmitter=self.process_name,
                        receiver=msg.transmitter,
                        uuid=msg.uuid,
                        func_name=msg.func_name,
                        return_result=False,
                        error=error,
                        period=msg.period,
                    ).dumps(),
                )
            except Exception:
                logger.error(
                    f"Exception while sending error details from scheduler to remote process {msg.transmitter}"
                )
        else:
            await sleep(delta)
            await self._remote_func_executor(stream, msg, "at_time")

    @with_debug_trace
    async def _remote_func_executor(self, stream: SocketStream, message: Message, condition: str) -> bool:
        error = None
        success = True
        remote_callback = LOCAL_CALLBACK_MAPPING[message.func_name]
        args = message.func_args
        kwargs = message.func_kwargs

        try:
            if remote_callback.is_async:
                await remote_callback(*args, **kwargs)
            else:
                await run_in_threadpool(remote_callback, *args, **kwargs)
            logger.info(f"Callback {message.func_name!r} was completed successfully (condition={condition!r}).")
        except TypeError as e:
            if "were given" in str(e) or "got an unexpected" in str(e) or "missing" in str(e):
                info = f"{e}. Function signature is: {message.func_name}{remote_callback.signature}: ..."
                success = False
            else:
                info = f"Exception while processing function {message.func_name!r} on remote executor {self.process_name!r} (condition={condition!r})."
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))
        except Exception as e:
            info = f"Exception while processing function {message.func_name!r} on remote executor {self.process_name!r} (condition={condition!r})."
            logger.error(info + f" Error = {e}")
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))

        if error:
            try:
                await send_to_stream(
                    stream,
                    Message(
                        flag=MessageFlag.SCHEDULER_ERROR,
                        transmitter=self.process_name,
                        receiver=message.transmitter,
                        uuid=message.uuid,
                        func_name=message.func_name,
                        return_result=False,
                        error=error,
                        period=message.period,
                    ).dumps(),
                )
            except Exception:
                logger.error(
                    f"Exception while sending error details from scheduler to remote process {message.transmitter}"
                )
        return success
