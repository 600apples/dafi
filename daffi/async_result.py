import time
from abc import ABC, abstractmethod
from queue import Queue, Empty
from dataclasses import dataclass
from threading import Event as thEvent
from typing import ClassVar, Optional, Union, NoReturn, Type, Any, Tuple

from anyio import sleep

from daffi.components.proto.message import RpcMessage
from daffi.exceptions import RemoteError, TimeoutError, InitializationError
from daffi.utils.custom_types import RemoteResult, SchedulerTaskType
from daffi.store import TttStore


__all__ = ["AsyncResult", "SchedulerTask"]


class ResultInf(ABC):
    _awaited_results: ClassVar[TttStore] = TttStore()

    @abstractmethod
    def ready(self):
        """Indicates if result is ready"""

    @abstractmethod
    def error(self):
        """Indicates if remote execution failed with error"""

    @abstractmethod
    def get(self):
        """Get result value"""

    @abstractmethod
    def get_async(self):
        """Get result value in async mode"""

    @abstractmethod
    def _register(self, *args, **kwargs):
        """Register new result waiter"""

    @abstractmethod
    def _clone_and_register(self, *args, **kwargs):
        """Clone result from existing instance in register result waiter"""

    @abstractmethod
    def _set(self, *args, **kwargs):
        """Notify that result waiter can obtain result value or error"""

    @classmethod
    def _set_and_trigger(cls, msg_uuid: int, result: Any, *args, **kwargs) -> bool:
        """Register result for result waiter and notify result waiter that result is ready to be taken"""
        if ares := cls._awaited_results.get(msg_uuid):
            if isinstance(ares, cls):
                ares._set_and_trigger(msg_uuid, result, *args, **kwargs)
            return True
        return False


@dataclass
class AsyncResult(ResultInf):
    _ipc: ClassVar["Ipc"] = None

    msg: RpcMessage
    result: Optional[RemoteResult] = None
    _timeout: Optional[Union[int, float]] = None

    def __post_init__(self):
        super().__init__()
        self._ready = thEvent()

    def __str__(self):
        return f"{self.__class__.__name__} (func_name={self.func_name}, uuid={self.uuid})"

    __repr__ = __str__

    def __hash__(self):
        return hash(f"{self.func_name}{self.uuid}")

    @property
    def logger(self):
        return self._ipc.logger

    @property
    def node(self):
        return self._ipc.node

    @property
    def func_name(self):
        return self.msg.func_name

    @property
    def uuid(self):
        return self.msg.uuid

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
        # `timeout` from arguments take precedence over class attribute `_timeout`
        timeout = timeout or self._timeout

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
        # `timeout` from arguments take precedence over class attribute `_timeout`
        timeout = timeout or self._timeout

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
        result_copy = self.__class__(msg=self.msg, _timeout=self._timeout)
        result_copy._register()
        return result_copy

    def _set(self):
        self._ready.set()

    @classmethod
    def _set_and_trigger(cls, msg_uuid: int, result: Any, *_, **__) -> NoReturn:
        ares = AsyncResult._awaited_results[msg_uuid]
        AsyncResult._awaited_results[msg_uuid] = result
        ares._set()


@dataclass
class SchedulerTask(AsyncResult):
    _transmitter: str = None
    _scheduler_type: SchedulerTaskType = None

    def cancel(self) -> NoReturn:
        self._ipc.cancel_scheduler(remote_process=self._transmitter, msg_uuid=self.uuid, func_name=self.func_name)

    def get(self) -> "SchedulerTask":
        self._transmitter = super().get()
        return self

    async def get_async(self) -> "SchedulerTask":
        self._transmitter = await super().get_async()
        return self


class IterableAsyncResult(AsyncResult):
    def __post_init__(self):
        self._queue = Queue()

    def __iter__(self):
        """Iterate over result items."""
        yield from self.get()

    @property
    def ready(self):
        raise NotImplementedError()

    @property
    def error(self):
        raise NotImplementedError()

    def get(self, timeout: Union[int, float] = None) -> RemoteResult:
        # `timeout` from arguments take precedence over class attribute `_timeout`
        timeout = timeout or self._timeout

        if self.result:
            InitializationError("Generator has already been used").fire()

        self.result = True
        if timeout:
            timeout_sentinel = time.time() + timeout

        try:
            while True:
                wait = None if timeout is None else timeout_sentinel - time.time()
                try:
                    result, completed = self._queue.get(timeout=wait)
                except Empty:
                    TimeoutError(f"Function {self.func_name} result timed out").fire()
                else:
                    if isinstance(result, RemoteError):
                        result.raise_with_trackeback()

                    if completed:
                        break

                    yield result

        finally:
            self.result = self._awaited_results.pop(self.uuid)

    def get_async(self, timeout: Union[int, float] = None) -> RemoteResult:
        # TODO need to find better way to handle queue items across threads for async applications.
        raise NotImplementedError()

    @classmethod
    def _set_and_trigger(cls, msg_uuid: int, result: Any, completed: Optional[bool] = True) -> NoReturn:
        AsyncResult._awaited_results[msg_uuid]._set((result, completed))

    def _set(self, item: Tuple):
        self._queue.put_nowait(item)


def get_result_type(
    is_period: bool, is_generator: bool
) -> Type[Union[AsyncResult, SchedulerTask, IterableAsyncResult]]:
    if is_generator:
        return IterableAsyncResult
    return SchedulerTask if is_period else AsyncResult
