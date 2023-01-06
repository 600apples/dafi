from anyio import to_thread
from dataclasses import dataclass, field
from threading import Event as thEvent
from typing import ClassVar, Dict, Optional, Union, NoReturn, Type, Any

from dafi.exceptions import RemoteError, TimeoutError, RemoteStoppedUnexpectedly, GlobalContextError
from dafi.utils.custom_types import RemoteResult, SchedulerTaskType
from dafi.utils.misc import async_library


@dataclass
class AsyncResult:
    _awaited_results: ClassVar[Dict[str, "AsyncResult"]] = dict()

    func_name: str
    uuid: str
    result: Optional[RemoteResult] = None
    async_context: Optional[str] = field(default_factory=async_library)

    def __post_init__(self):
        self._ready = thEvent()

    async def __self_await__(self):
        return self

    def __await__(self):
        return self.__self_await__().__await__()

    def __and__(self, other):
        return self

    @classmethod
    async def _fold_results(cls):
        for msg_uuid, ares in AsyncResult._awaited_results.items():
            AsyncResult._awaited_results[msg_uuid] = RemoteError(
                info="Lost connection to Controller.",
                _awaited_error_type=RemoteStoppedUnexpectedly,
            )
            ares.set()

    @classmethod
    async def _set_and_trigger(cls, msg_uuid: int, result: Any) -> NoReturn:
        ares = AsyncResult._awaited_results[msg_uuid]
        AsyncResult._awaited_results[msg_uuid] = result
        ares.set()

    def __call__(self, timeout: Union[int, float] = None) -> RemoteResult:
        return self.get(timeout=timeout)

    def set(self):
        self._ready.set()

    def get(self, timeout: Union[int, float] = None) -> RemoteResult:
        if self.async_context:
            return self._get_async(timeout=timeout)
        return self._get(timeout=timeout)

    def _get(self, timeout: Union[int, float] = None) -> RemoteResult:
        if self.result is None:
            self._ready.wait(timeout=timeout)
            self.result = self._awaited_results.pop(self.uuid)

            if isinstance(self.result, RemoteError):
                self.result.raise_with_trackeback()

            if self.result == self:
                self.result = None
                raise TimeoutError(f"Function {self.func_name} result timed out")

        return self.result

    async def _get_async(self, timeout: Union[int, float] = None) -> RemoteResult:
        if self.result is None:
            await to_thread.run_sync(self._ready.wait, timeout)
            self.result = self._awaited_results.pop(self.uuid)

            if isinstance(self.result, RemoteError):
                self.result.raise_with_trackeback()

            if self.result == self:
                self.result = None
                raise TimeoutError(f"Function {self.func_name} result timed out")

        return self.result


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


def get_result_type(inside_callback_context: bool, is_period: bool) -> Type[Union[AsyncResult, SchedulerTask]]:
    if inside_callback_context and is_period:
        raise GlobalContextError("Initialization of periodic tasks from the context of a remote callback is prohibited")
    return SchedulerTask if is_period else AsyncResult
