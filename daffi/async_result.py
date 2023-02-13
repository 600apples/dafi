import time
import logging
from dataclasses import dataclass
from threading import Event as thEvent
from typing import ClassVar, Optional, Union, NoReturn, Type, Any, Tuple, Callable

from anyio import sleep

from daffi.utils.misc import Observer
from daffi.components.proto.message import RpcMessage
from daffi.exceptions import RemoteError, TimeoutError, GlobalContextError
from daffi.utils.custom_types import RemoteResult, SchedulerTaskType
from daffi.exceptions import InitializationError, RemoteStoppedUnexpectedly
from daffi.utils.misc import iterable
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
    stop_any,
    RetryCallState,
)
from daffi.store import TttStore


__all__ = ["RetryPolicy", "AsyncResult", "SchedulerTask"]


@dataclass
class RetryPolicy:
    acceptable_errors: Union[Type[BaseException], Tuple[Type[BaseException]]]
    stop_after_attempt: Optional[int] = None
    stop_after_delay: Optional[int] = None
    wait: Optional[Union[int, float]] = 5
    str_expression: Optional[str] = None

    def __post_init__(self):
        self.str_expression = {self.str_expression} if self.str_expression else set()
        self.acceptable_errors = set(self.acceptable_errors)

    def __add__(self, other: "RetryPolicy"):
        if other:
            self.acceptable_errors |= other.acceptable_errors
            self.str_expression |= other.str_expression

            if other.stop_after_delay:
                self.stop_after_attempt = None
            self.stop_after_attempt = other.stop_after_attempt or self.stop_after_attempt
            self.stop_after_delay = other.stop_after_delay or self.stop_after_delay
            self.wait = other.wait or self.wait

    def wrap(self, fn: Callable[..., Any], on_retry: Callable[[RetryCallState], Any]) -> Callable[..., Any]:
        if not self.acceptable_errors:
            raise InitializationError("acceptable_errors argument is required")
        if not iterable(self.acceptable_errors):
            self.acceptable_errors = (self.acceptable_errors,)

        _stop = stop_any()
        if self.stop_after_attempt:
            _stop = stop_after_attempt(self.stop_after_attempt)
        elif self.stop_after_delay:
            _stop = stop_after_delay(self.stop_after_delay)

        return retry(
            reraise=True,
            retry=retry_if_exception(self.on_exception),
            after=on_retry,
            wait=wait_fixed(self.wait),
            stop=_stop,
        )(fn)

    def on_exception(self, exc: Type[BaseException]) -> bool:
        if type(exc) in self.acceptable_errors:
            if self.str_expression:
                for expr in self.str_expression:
                    if expr in str(exc):
                        return True
            else:
                return True
        return False


@dataclass
class AsyncResult(Observer):
    _ipc: ClassVar["Ipc"] = None
    _awaited_results: ClassVar[TttStore] = TttStore()

    msg: RpcMessage
    retry_policy: Optional[RetryPolicy] = None
    result: Optional[RemoteResult] = None

    def __post_init__(self):
        super().__init__()
        self._ready = thEvent()
        self.retry_policy = self.default_retry_policy + self.retry_policy
        self.get = self.default_retry_policy.wrap(self.get, self.on_retry)
        self.get_async = self.default_retry_policy.wrap(self.get_async, self.on_retry)

    def __str__(self):
        return f"{self.__class__.__name__} (func_name={self.func_name}, uuid={self.uuid})"

    def __hash__(self):
        return hash(f"{self.func_name}{self.uuid}")

    @property
    def default_retry_policy(self) -> RetryPolicy:
        """Default retry policy that covers state when node lost connection to controller."""
        return RetryPolicy(
            stop_after_attempt=5,
            wait=2,
            str_expression="Lost connection to Controller",
            acceptable_errors={RemoteStoppedUnexpectedly},
        )

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

    def on_retry(self, retry_state: RetryCallState):
        self.logger.log(
            logging.WARNING,
            f"Finished call to '{self.func_name}' "
            f"after {retry_state.seconds_since_start}(s), "
            f"this was the {retry_state.attempt_number} time calling it.",
        )
        self._ready = thEvent()
        self.result = None
        self._register()
        self.node.send_threadsave(self.msg, 0)

    def get(self, timeout: Union[int, float] = None) -> RemoteResult:
        if self.result is None:
            self._ready.wait(timeout=timeout)
            self.result = self._awaited_results.pop(self.uuid)

            if isinstance(self.result, RemoteError):
                self.mark_fail()
                self.result.raise_with_trackeback()

            if self.result == self:
                self.mark_fail()
                self.result = None
                TimeoutError(f"Function {self.func_name} result timed out").fire()

        self.mark_done()
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
        result_copy = self.__class__(msg=self.msg)
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


def get_result_type(inside_callback_context: bool, is_period: bool) -> Type[Union[AsyncResult, SchedulerTask]]:
    if inside_callback_context and is_period:
        GlobalContextError(
            "Initialization of periodic tasks from the context of a remote callback is prohibited"
        ).fire()
    return SchedulerTask if is_period else AsyncResult
