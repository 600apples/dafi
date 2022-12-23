import time
from dataclasses import dataclass, field
from datetime import timedelta
from threading import Event
from typing import Optional, Union, NoReturn, Callable, Any, Coroutine

from anyio import to_thread
from dafi.async_result import AsyncResult
from dafi.exceptions import GlobalContextError
from dafi.utils.timeparse import timeparse
from dafi.utils.custom_types import P, RemoteResult
from dafi.utils.mappings import search_cb_info_in_mapping, NODE_CALLBACK_MAPPING
from dafi.utils.misc import async_library


@dataclass
class FG:
    timeout: Optional[Union[timedelta, int, float, str]] = None


@dataclass
class BG:
    timeout: Optional[Union[timedelta, int, float, str]] = None
    eta: Optional[Union[timedelta, int, float, str]] = None


@dataclass
class NO_RETURN:
    eta: Optional[Union[timedelta, int, float, str]] = None


@dataclass
class RemoteCall:
    _ipc: "Ipc"
    func_name: Optional[str] = None
    args: P.args = field(default_factory=tuple)
    kwargs: P.kwargs = field(default_factory=dict)

    def __str__(self):
        sep = ", "
        args = sep.join(map(str, self.args))
        kwargs = sep.join(f", {k}={v}" for k, v in self.kwargs.items())
        args_kwargs = f"{args}{kwargs}".strip(",").strip()
        return f"<{self.__class__.__name__} {self.func_name}({args_kwargs})>"

    async def __self_await__(self):
        return self

    def __await__(self):
        return self.__self_await__().__await__()

    def __and__(self, other) -> RemoteResult:
        if type(other) == type:
            other = other()

        if isinstance(other, (FG, type(FG))):
            return self.fg(timeout=other.timeout)

        elif isinstance(other, (BG, type(BG))):
            return self.bg(timeout=other.timeout, eta=other.eta)

        elif isinstance(other, (NO_RETURN, type(NO_RETURN))):
            return self.no_return(eta=NO_RETURN.eta)

        else:
            valid_operands = ", ".join(map(lambda o: o.__name__, (FG, BG, NO_RETURN)))
            raise GlobalContextError(f"Invalid operand {type(other)}. Use one of {valid_operands}")

    def __call__(self, timeout: Optional[Union[timedelta, int, float, str]] = None) -> RemoteResult:
        return self.fg(timeout=timeout)

    @property
    def info(self) -> Optional["CallbackInfo"]:
        data = search_cb_info_in_mapping(NODE_CALLBACK_MAPPING, self.func_name)
        if data:
            return data[1]

    @property
    def exists(self) -> bool:
        return bool(self.info)

    def fg(self, timeout: Optional[Union[timedelta, int, float, str]] = None) -> RemoteResult:
        timeout = self._get_duration(timeout, "timeout")
        return self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            timeout=timeout,
            async_=False,
            return_result=True,
        )

    def bg(
        self, timeout: Optional[Union[int, float]] = None, eta: Optional[Union[timedelta, int, float, str]] = None
    ) -> AsyncResult:
        timeout = self._get_duration(timeout, "timeout")
        eta = self._get_duration(eta, "eta", 0)
        return self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            timeout=timeout,
            eta=eta,
            async_=True,
            return_result=True,
        )

    def no_return(self, eta: Optional[Union[timedelta, int, float, str]] = None) -> NoReturn:
        eta = self._get_duration(eta, "eta", 0)
        self._ipc.call(self.func_name, args=self.args, kwargs=self.kwargs, eta=eta, async_=True, return_result=False)

    def _get_duration(
        self, val: Union[timedelta, int, float, str], arg_name: str, default: Optional[int] = None
    ) -> Union[int, float]:
        if val:
            if isinstance(val, timedelta):
                val = val.total_seconds()
            elif isinstance(val, str):
                val = timeparse(val)
            elif not isinstance(val, (float, int)):
                raise GlobalContextError(
                    f"Invalid {arg_name} format. Should be string, int, float or datetime.timedelta."
                )

            if val < 0:
                raise GlobalContextError(f"{arg_name} cannot be negative.")
        return val or default


@dataclass
class LazyRemoteCall:
    _ipc: "Ipc" = field(repr=False)
    _stop_event: Event = field(repr=False)
    func_name: Optional[str] = None

    @property
    def info(self):
        if not self.func_name:
            raise GlobalContextError(
                "Not allowed in current context."
                " Use `Global.call.func_name(*args, **kwargs)` to build RemoteCall instance."
            )
        return RemoteCall(_ipc=self._ipc, func_name=self.func_name).info

    @property
    def exist(self) -> bool:
        return bool(self.info)

    def wait_function(self) -> Union[NoReturn, Coroutine]:
        interval = 0.5
        condition_executable = lambda: self.exist
        if async_library():
            return to_thread.run_sync(self._wait, condition_executable, interval)
        else:
            self._wait(condition_executable, interval)

    def wait_process(self, process_name: str) -> Union[NoReturn, Coroutine]:
        interval = 0.5
        condition_executable = lambda: process_name in NODE_CALLBACK_MAPPING
        if async_library():
            return to_thread.run_sync(self._wait, condition_executable, interval)
        else:
            self._wait(condition_executable, interval)

    def __call__(__self__, *args, **kwargs) -> RemoteCall:
        if not __self__.func_name:
            raise GlobalContextError(
                "Invalid syntax. Use `Global.call.func_name(*args, **kwargs)` to build RemoteCall instance."
            )
        return RemoteCall(_ipc=__self__._ipc, func_name=__self__.func_name, args=args, kwargs=kwargs)

    def __getattr__(self, item) -> RemoteCall:
        if self._stop_event.is_set():
            raise GlobalContextError("Global can no longer accept remote calls because it was stopped")
        self.func_name = item
        return self

    def _wait(self, condition_executable: Callable, interval: float) -> NoReturn:
        while True:
            if condition_executable():
                break
            time.sleep(interval)
