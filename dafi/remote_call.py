import time
import asyncio
from dataclasses import dataclass, field
from datetime import timedelta, datetime
from threading import Event
from typing import Optional, Union, NoReturn, Callable, Coroutine, List

from dafi.async_result import AsyncResult, SchedulerTask, AsyncSchedulerTask
from dafi.exceptions import GlobalContextError
from dafi.utils.timeparse import timeparse
from dafi.utils.custom_types import P, RemoteResult, TimeUnits
from dafi.utils.settings import search_remote_callback_in_mapping, NODE_CALLBACK_MAPPING
from dafi.utils.misc import sync_to_async, Period


@dataclass
class FG:
    timeout: Optional[TimeUnits] = None


@dataclass
class BG:
    timeout: Optional[TimeUnits] = None
    eta: Optional[TimeUnits] = None


@dataclass
class NO_RETURN:
    eta: Optional[TimeUnits] = None


@dataclass
class PERIOD:
    at_time: Optional[Union[List[TimeUnits], TimeUnits]] = None
    period: Optional[TimeUnits] = None


@dataclass
class BROADCAST:
    eta: Optional[TimeUnits] = None


@dataclass
class RemoteCall:
    _ipc: "Ipc" = field(repr=False)
    func_name: Optional[str] = None
    args: P.args = field(default_factory=tuple)
    kwargs: P.kwargs = field(default_factory=dict)
    _inside_callback_context: Optional[bool] = field(repr=False, default=False)

    def __str__(self):
        sep = ", "
        args = sep.join(map(str, self.args))
        kwargs = sep.join(f", {k}={v}" for k, v in self.kwargs.items())
        args_kwargs = f"{args}{kwargs}".strip(",").strip()
        return f"<{self.__class__.__name__} {self.func_name}({args_kwargs})>"

    async def __self_await__(self):
        return self

    def __await__(self):
        self._awaitable = True
        return self.__self_await__().__await__()

    def __and__(self, other) -> Union[RemoteResult, Coroutine]:
        if type(other) == type:
            other = other()

        if isinstance(other, (FG, type(FG))):
            if self._inside_callback_context:
                raise GlobalContextError("FG is not available inside callback context. Use .fg instead.")
            return self.fg(timeout=other.timeout)

        elif isinstance(other, (BG, type(BG))):
            if self._inside_callback_context:
                return sync_to_async(self.bg)(timeout=other.timeout, eta=other.eta)
            return self.bg(timeout=other.timeout, eta=other.eta)

        elif isinstance(other, (NO_RETURN, type(NO_RETURN))):
            return self.no_return(eta=other.eta)

        elif isinstance(other, (PERIOD, type(PERIOD))):
            return self.period(at_time=other.at_time, period=other.period)

        elif isinstance(other, (BROADCAST, type(BROADCAST))):
            return self.broadcast(eta=other.eta)

        else:
            valid_operands = ", ".join(map(lambda o: o.__name__, (FG, BG, NO_RETURN, PERIOD, BROADCAST)))
            raise GlobalContextError(f"Invalid operand {type(other)}. Use one of {valid_operands}")

    def __call__(self, timeout: Optional[TimeUnits] = None) -> RemoteResult:
        return self.fg(timeout=timeout)

    @property
    def info(self) -> Optional["RemoteCallback"]:
        data = search_remote_callback_in_mapping(NODE_CALLBACK_MAPPING, self.func_name)
        if data:
            return data[1]

    @property
    def exists(self) -> bool:
        return bool(self.info)

    def fg(self, timeout: Optional[TimeUnits] = None) -> RemoteResult:
        timeout = self._get_duration(timeout, "timeout")
        return self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            timeout=timeout,
            async_=False,
            return_result=True,
            inside_callback_context=self._inside_callback_context,
        )

    def bg(self, timeout: Optional[TimeUnits] = None, eta: Optional[TimeUnits] = None) -> AsyncResult:
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
            inside_callback_context=self._inside_callback_context,
        )

    def no_return(self, eta: Optional[TimeUnits] = None) -> NoReturn:
        eta = self._get_duration(eta, "eta", 0)
        res = self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            eta=eta,
            async_=True,
            return_result=False,
            inside_callback_context=self._inside_callback_context,
        )
        if asyncio.iscoroutine(res) and self._inside_callback_context:
            asyncio.create_task(res)

    def broadcast(self, eta: Optional[TimeUnits] = None) -> NoReturn:
        eta = self._get_duration(eta, "eta", 0)
        res = self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            eta=eta,
            async_=True,
            return_result=False,
            broadcast=True,
            inside_callback_context=self._inside_callback_context,
        )
        if asyncio.iscoroutine(res) and self._inside_callback_context:
            asyncio.create_task(res)

    def period(
        self,
        at_time: Optional[Union[List[TimeUnits], TimeUnits]],
        period: Optional[TimeUnits],
    ) -> Union[SchedulerTask, AsyncSchedulerTask]:
        if at_time is not None:
            if isinstance(at_time, (int, float, str, timedelta, datetime)):
                at_time = [at_time]
            at_time = [self._get_duration(i, "at_time", 0) for i in at_time]
        if period is not None:
            period = self._get_duration(period, "period", 0)

        func_period = Period(at_time=at_time, period=period)
        func_period.validate()

        res = self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            async_=True,
            return_result=False,
            func_period=func_period,
            inside_callback_context=self._inside_callback_context,
        )
        if asyncio.iscoroutine(res) and self._inside_callback_context:
            return asyncio.create_task(res)
        return res

    def _get_duration(self, val: TimeUnits, arg_name: str, default: Optional[int] = None) -> Union[int, float]:
        if val:
            if isinstance(val, timedelta):
                val = val.total_seconds()
            elif isinstance(val, str):
                val = timeparse(val)
            elif isinstance(val, datetime):
                val = time.mktime(val.timetuple())
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
    _global_terminate_event: Event = field(repr=False)
    _func_name: Optional[str] = field(repr=False, default=None)
    _inside_callback_context: Optional[bool] = field(repr=False, default=False)

    def __str__(self):
        return self.__class__.__name__ if not self._func_name else f"{self.__class__.__name__}({self._func_name})"

    @property
    def _info(self):
        if self._func_name is None:
            raise GlobalContextError(
                "Not allowed in current context."
                " Use `Global.call.func_name(*args, **kwargs)` to build RemoteCall instance."
            )
        elif self._func_name == "":
            raise GlobalContextError("Empty function name is not allowed.")
        return RemoteCall(_ipc=self._ipc, func_name=self._func_name).info

    @property
    def _exist(self) -> bool:
        return bool(self._info)

    def _wait_function(self) -> Union[NoReturn, Coroutine]:
        interval = 0.5

        def condition_executable():
            return self._exist

        self._wait(condition_executable, interval)

    def _wait_process(self, process_name: str) -> NoReturn:
        interval = 0.5

        def condition_executable():
            return process_name in NODE_CALLBACK_MAPPING

        self._wait(condition_executable, interval)

    def __call__(__self__, *args, **kwargs) -> RemoteCall:
        if not __self__._func_name:
            raise GlobalContextError(
                "Invalid syntax. Use `Global.call.func_name(*args, **kwargs)` to build RemoteCall instance."
            )
        return RemoteCall(
            _ipc=__self__._ipc,
            func_name=__self__._func_name,
            args=args,
            kwargs=kwargs,
            _inside_callback_context=__self__._inside_callback_context,
        )

    def __getattr__(self, item) -> RemoteCall:
        if self._global_terminate_event and self._global_terminate_event.is_set():
            raise GlobalContextError("Global can no longer accept remote calls because it was stopped")
        self._func_name = item
        return self

    def _wait(self, condition_executable: Callable, interval: float) -> NoReturn:
        while True:
            if condition_executable():
                break
            time.sleep(interval)
