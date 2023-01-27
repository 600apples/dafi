import sys
import pickle
import logging
import asyncio
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Deque
from collections import namedtuple, deque

from anyio import sleep

from daffi.utils import colors
from daffi.components.operations.channel_store import ChannelPipe
from daffi.components.proto.message import RpcMessage, RemoteError, MessageFlag
from daffi.utils.logger import patch_logger
from daffi.utils.misc import run_in_threadpool, run_from_working_thread

from daffi.utils.settings import LOCAL_CALLBACK_MAPPING


logger = patch_logger(logging.getLogger("scheduler"), colors.magenta)

TaskIdent = namedtuple("TaskIdent", ("process_name", "func_name", "msg_uuid"))

SCHEDULER_PERIODICAL_TASKS: Dict[TaskIdent, asyncio.Task] = dict()
SCHEDULER_AT_TIME_TASKS: Dict[TaskIdent, List[asyncio.Task]] = dict()
FINISHED_TASKS: Deque[int] = deque(maxlen=1000)


@dataclass
class Scheduler:
    process_name: str
    async_backend: str

    async def on_scheduler_stop(self):
        try:
            for task_group in SCHEDULER_AT_TIME_TASKS.values():
                for task in filter(None, task_group):
                    task.cancel()
            for task in SCHEDULER_PERIODICAL_TASKS.values():
                task.cancel()
        except RuntimeError:
            ...
        FINISHED_TASKS.clear()
        SCHEDULER_PERIODICAL_TASKS.clear()
        SCHEDULER_AT_TIME_TASKS.clear()

    async def on_error(self, msg: RpcMessage):
        msg.error.show_in_log(logger=logger)

    async def register(self, msg: RpcMessage, channel: ChannelPipe):

        task_ident = TaskIdent(msg.transmitter, msg.func_name, msg.uuid)
        if msg.period.interval:
            SCHEDULER_PERIODICAL_TASKS[task_ident] = asyncio.create_task(
                self.on_interval(msg.period.interval, channel, msg, task_ident)
            )

        elif msg.period.at_time:
            SCHEDULER_AT_TIME_TASKS[task_ident] = [
                asyncio.create_task(self.on_at_time(ts, channel, msg, task_ident, ind))
                for ind, ts in enumerate(msg.period.at_time)
            ]

    async def on_interval(self, interval: int, channel: ChannelPipe, msg: RpcMessage, task_ident: TaskIdent):
        while True:
            await sleep(interval)
            if not await self._remote_func_executor(channel, msg, "interval"):
                logger.error(
                    f"Callback {msg.func_name} cannot be executed periodically"
                    f" since unrecoverable error detected. Stop scheduling..."
                )
                SCHEDULER_PERIODICAL_TASKS.pop(task_ident, None)
                FINISHED_TASKS.append(task_ident.msg_uuid)
                break

    async def on_at_time(self, ts: int, channel: ChannelPipe, msg: RpcMessage, task_ident: TaskIdent, task_index: int):
        now = datetime.utcnow().timestamp()
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
        at_time_list = SCHEDULER_AT_TIME_TASKS.get(task_ident)
        if at_time_list:
            at_time_list[task_index] = None
            FINISHED_TASKS.append(task_ident.msg_uuid)
            if all(task is None for task in at_time_list):
                # Remove at time tasks list only when last task is completed
                SCHEDULER_AT_TIME_TASKS.pop(task_ident, None)

    async def _remote_func_executor(self, channel: ChannelPipe, msg: RpcMessage, condition: str) -> bool:
        error = None
        success = True
        remote_callback = LOCAL_CALLBACK_MAPPING.get(msg.func_name)
        if not remote_callback:
            return False

        args = msg.func_args
        kwargs = msg.func_kwargs

        try:
            if remote_callback.is_async:
                await run_from_working_thread(self.async_backend, remote_callback, *args, **kwargs)
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
