import sys
import time
import pickle
import logging
import asyncio
from dataclasses import dataclass

from anyio import sleep
from anyio._backends._asyncio import TaskGroup

from dafi.utils import colors
from dafi.components.operations.channel_store import ChannelPipe
from dafi.components.proto.message import RpcMessage, RemoteError, MessageFlag
from dafi.utils.logger import patch_logger
from dafi.utils.misc import run_in_threadpool
from dafi.utils.debug import with_debug_trace
from dafi.utils.settings import LOCAL_CALLBACK_MAPPING, SCHEDULER_PERIODICAL_TASKS, SCHEDULER_AT_TIME_TASKS


logger = patch_logger(logging.getLogger(__name__), colors.magenta)


@dataclass
class Scheduler:
    sg: TaskGroup
    process_name: str

    async def on_scheduler_stop(cls):
        for task_group in SCHEDULER_AT_TIME_TASKS.values():
            for task in task_group:
                task.cancel()
        for task in SCHEDULER_PERIODICAL_TASKS.values():
            task.cancel()

    @with_debug_trace
    async def on_error(self, msg: RpcMessage):
        msg.error.show_in_log(logger=logger)

    @with_debug_trace
    async def register(self, msg: RpcMessage, channel: ChannelPipe):

        if msg.period.interval:
            if msg.func_name in SCHEDULER_PERIODICAL_TASKS:
                SCHEDULER_PERIODICAL_TASKS[msg.func_name].cancel()
            SCHEDULER_PERIODICAL_TASKS[msg.func_name] = asyncio.create_task(
                self.on_interval(msg.period.interval, channel, msg)
            )

        elif msg.period.at_time:
            SCHEDULER_AT_TIME_TASKS[msg.uuid] = [
                asyncio.create_task(self.on_at_time(ts, channel, msg)) for ts in msg.period.at_time
            ]

    @with_debug_trace
    async def on_interval(self, interval: int, channel: ChannelPipe, msg: RpcMessage):
        while True:
            await sleep(interval)
            if not await self._remote_func_executor(channel, msg, "interval"):
                logger.error(
                    f"Callback {msg.func_name} cannot be executed periodically"
                    f" since unrecoverable error detected. Stop scheduling..."
                )
                SCHEDULER_PERIODICAL_TASKS.pop(msg.uuid, None)
                break

    @with_debug_trace
    async def on_at_time(self, ts: int, channel: ChannelPipe, msg: RpcMessage):
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
                channel.send(
                    RpcMessage(
                        flag=MessageFlag.SCHEDULER_ERROR,
                        transmitter=self.process_name,
                        receiver=msg.transmitter,
                        uuid=msg.uuid,
                        func_name=msg.func_name,
                        return_result=False,
                        error=error,
                        period=msg.period,
                    )
                )
            except Exception:
                logger.error(
                    f"Exception while sending error details from scheduler to remote process {msg.transmitter}"
                )
        else:
            await sleep(delta)
            await self._remote_func_executor(channel, msg, "at_time")
        SCHEDULER_AT_TIME_TASKS.pop(msg.uuid, None)

    @with_debug_trace
    async def _remote_func_executor(self, channel: ChannelPipe, msg: RpcMessage, condition: str) -> bool:
        error = None
        success = True
        remote_callback = LOCAL_CALLBACK_MAPPING[msg.func_name]
        args = msg.func_args
        kwargs = msg.func_kwargs

        try:
            if remote_callback.is_async:
                await remote_callback(*args, **kwargs)
            else:
                await run_in_threadpool(remote_callback, *args, **kwargs)
            logger.info(f"Callback {msg.func_name!r} was completed successfully (condition={condition!r}).")
        except TypeError as e:
            if "were given" in str(e) or "got an unexpected" in str(e) or "missing" in str(e):
                info = f"{e}. Function signature is: {msg.func_name}{remote_callback.signature}: ..."
                success = False
            else:
                info = (
                    f"Exception while processing function {msg.func_name!r} "
                    f"on remote executor {self.process_name!r} (condition={condition!r})."
                )
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))
        except Exception as e:
            info = (
                f"Exception while processing function {msg.func_name!r}"
                f" on remote executor {self.process_name!r} (condition={condition!r})."
            )
            logger.error(info + f" Error = {e}")
            error = RemoteError(info=info, traceback=pickle.dumps(sys.exc_info()))

        if error:
            try:
                channel.send(
                    RpcMessage(
                        flag=MessageFlag.SCHEDULER_ERROR,
                        transmitter=self.process_name,
                        receiver=RpcMessage.transmitter,
                        uuid=RpcMessage.uuid,
                        func_name=RpcMessage.func_name,
                        return_result=False,
                        error=error,
                        period=RpcMessage.period,
                    )
                )
            except Exception:
                logger.error(
                    f"Exception while sending error details from scheduler to remote process {RpcMessage.transmitter}"
                )
        return success
