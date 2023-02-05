import time
from typing import NoReturn
from anyio import create_task_group
from anyio.abc import TaskStatus
from anyio import TASK_STATUS_IGNORED

from daffi.utils import colors
from daffi.utils.logger import get_daffi_logger
from daffi.components.operations.freezable_queue import FreezableQueue, QueueMixin
from daffi.components.proto.message import RpcMessage
from daffi.async_result import AsyncResult


class TaskWaiter(QueueMixin):
    """
    Helper class for waiting result in the background.
    For those methods which are not expect wait for result it is the way to notify requester process
    about errors on remote side.
    """

    async def __call__(self, task_status: TaskStatus = TASK_STATUS_IGNORED):
        """
        TaskWaiter entrypoint method.
        Iterate over processed results and wait each of them in separate coroutine.
        """
        task_status.started("STARTED")

        self.logger = get_daffi_logger(self.__class__.__name__.lower(), colors.cyan)
        self.awaited_results = dict()
        self.q = FreezableQueue()

        async with create_task_group() as sg:
            async for result in self.q.iterate():
                self.awaited_results[result] = True
                sg.start_soon(self.wait_result, result)

    def stop(self) -> NoReturn:
        """Stop waiting tasks"""
        self.stop_threadsave()

    def register_result(self, msg: RpcMessage):
        """Register new result"""
        result = AsyncResult(func_name=msg.func_name, uuid=msg.uuid)._register()
        self.send_threadsave(result)

    async def wait_result(self, result: AsyncResult) -> NoReturn:
        """
        Wait result.
        If remote returns error this error will be logged.
        """
        try:
            await result.get_async()
        except Exception as e:
            self.logger.error(f"Error on remote: {e}")
        self.awaited_results.pop(result, None)

    def wait_all_results(self) -> NoReturn:
        """Wait all results that are currently in progress."""
        while True:
            _awaited = [(r.uuid, r.func_name) for r in self.awaited_results]
            if not _awaited:
                break
            self.logger.info(f"Next message(s) are still awaited: {_awaited}")
            time.sleep(2)
