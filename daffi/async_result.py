import time
from dataclasses import dataclass
from threading import Event as thEvent
from typing import ClassVar, Dict, Optional, Union, NoReturn, Type, Any

from anyio import sleep

from daffi.exceptions import RemoteError, TimeoutError, GlobalContextError
from daffi.utils.custom_types import RemoteResult, SchedulerTaskType


@dataclass
class AsyncResult:
    _awaited_results: ClassVar[Dict[str, "AsyncResult"]] = dict()

    func_name: str
    uuid: int
    result: Optional[RemoteResult] = None

    def __post_init__(self):
        self._ready = thEvent()

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    @property
    def error(self):
        if not self.result:
            return
        res = self._awaited_results.get(self.uuid)
        if isinstance(res, RemoteError):
            return res

    def get(self, timeout: Union[int, float] = None) -> RemoteResult:
        if self.result is None:
            self._ready.wait(timeout=timeout)
            self.result = self._awaited_results.pop(self.uuid)

            if isinstance(self.result, RemoteError):
                self.result.raise_with_trackeback()

            if self.result == self:
                self.result = None
                TimeoutError(f"Function {self.func_name} result timed out").fire()

        return self.result

    async def get_async(self, timeout: Union[int, float] = None) -> RemoteResult:
        if self.result is None:
            if timeout is not None:
                timeout = time.time() + timeout

                def timeout_cond():
                    return time.time() < timeout

            else:

                def timeout_cond():
                    return True

            while timeout_cond() and not self._ready.is_set():
                await sleep(0.01)

            self.result = self._awaited_results.pop(self.uuid)

            if isinstance(self.result, RemoteError):
                self.result.raise_with_trackeback()

            if self.result == self:
                self.result = None
                TimeoutError(f"Function {self.func_name} result timed out").fire()

        return self.result

    def _register(self) -> "AsyncResult":
        AsyncResult._awaited_results[self.uuid] = self
        return self

    def _clone_and_register(self):
        result_copy = self.__class__(func_name=self.func_name, uuid=self.uuid)
        result_copy._register()
        return result_copy

    def _set(self):
        self._ready.set()

    @classmethod
    def _set_and_trigger(cls, msg_uuid: int, result: Any) -> NoReturn:
        ares = AsyncResult._awaited_results[msg_uuid]
        if isinstance(ares, AsyncResult):
            AsyncResult._awaited_results[msg_uuid] = result
            ares._set()


@dataclass
class BaseTask:
    _ipc: "Ipc" = None
    _transmitter: str = None
    _scheduler_type: SchedulerTaskType = None

    def cancel(self) -> NoReturn:
        self._ipc.cancel_scheduler(remote_process=self._transmitter, msg_uuid=self.uuid, func_name=self.func_name)


@dataclass
class SchedulerTask(BaseTask, AsyncResult):
    def get(self) -> "SchedulerTask":
        self._transmitter = super().get()
        return self

    async def get_async(self) -> "SchedulerTask":
        self._transmitter = await super().get_async()
        return self


def get_result_type(inside_callback_context: bool, is_period: bool) -> Type[Union[AsyncResult, SchedulerTask]]:
    if inside_callback_context and is_period:
        GlobalContextError(
            "Initialization of periodic tasks from the context of a remote callback is prohibited"
        ).fire()
    return SchedulerTask if is_period else AsyncResult
