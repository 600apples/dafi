import re
import sys
import time
from inspect import getframeinfo
from anyio import to_thread
from dataclasses import dataclass, field
from datetime import timedelta, datetime
from threading import Event
from typing import Optional, Union, NoReturn, Callable, List, Any

from daffi.async_result import AsyncResult, SchedulerTask
from daffi.exceptions import GlobalContextError, InitializationError
from daffi.utils.timeparse import timeparse
from daffi.utils.custom_types import P, RemoteResult, TimeUnits
from daffi.utils.misc import Period, search_remote_callback_in_mapping

__all__ = ["RemoteResult", "FG", "BG", "PERIOD", "BROADCAST", "NO_RETURN"]


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
    interval: Optional[TimeUnits] = None


@dataclass
class BROADCAST:
    eta: Optional[TimeUnits] = None
    timeout: Optional[TimeUnits] = None  # Works only with return_result=True
    return_result: Optional[bool] = False


_empty_result = object()
_available_operands = "|".join([o.__name__ for o in (FG, BG, NO_RETURN, PERIOD, BROADCAST)])
_available_operands_search_pattern = re.compile(f"\\s*&\\s*({_available_operands})\\s*(\\(.*?\\))?")


@dataclass
class RemoteCall:
    _ipc: "Ipc" = field(repr=False)
    func_name: Optional[str] = None
    args: P.args = field(default_factory=tuple)
    kwargs: P.kwargs = field(default_factory=dict)
    _inside_callback_context: Optional[bool] = field(repr=False, default=False)
    _result: Optional[Any] = field(repr=False, default=_empty_result)

    def __str__(self):
        sep = ", "
        args = sep.join(map(str, self.args))
        kwargs = sep.join(f", {k}={v}" for k, v in self.kwargs.items())
        args_kwargs = f"{args}{kwargs}".strip(",").strip()
        return f"<{self.__class__.__name__} {self.func_name}({args_kwargs})>"

    def obtain_result_in_thread(self, next_operand: Union[FG, BG, BROADCAST, NO_RETURN, PERIOD]):
        self._result = self & next_operand

    async def __self_await__(self, next_operand: Union[FG, BG, BROADCAST, NO_RETURN, PERIOD]):
        await to_thread.run_sync(self.obtain_result_in_thread, next_operand)
        return self

    def __await__(self):
        frame = sys._getframe(1)
        info = getframeinfo(frame)
        code_context = info.code_context
        frame_locals = frame.f_locals
        frame_globals = frame.f_globals
        try:
            next_operand, next_operand_args = re.findall(_available_operands_search_pattern, "".join(code_context))[0]
        except (ValueError, IndexError):
            raise InitializationError(
                f"Object of {self.__class__.__name__} doesn't support await expressions. "
                f"Use await g.call.<callback_name> & {_available_operands}[(*args, **kwargs)].\n"
                f"Do not use pre-initialization of operands either.\n"
                f"For instance this is wrong:\nop=BG(eta=15)\nawait g.call.<callback_name> & op\n"
            )
        next_operand = eval(f"{next_operand}{next_operand_args}", frame_globals, frame_locals)
        return self.__self_await__(next_operand).__await__()

    def __and__(self, other) -> RemoteResult:
        if self._result != _empty_result:
            return self._result

        if type(other) == type:
            other = other()

        if isinstance(other, (FG, type(FG))):
            return self.fg(timeout=other.timeout)

        elif isinstance(other, (BG, type(BG))):
            return self.bg(timeout=other.timeout, eta=other.eta)

        elif isinstance(other, (NO_RETURN, type(NO_RETURN))):
            return self.no_return(eta=other.eta)

        elif isinstance(other, (PERIOD, type(PERIOD))):
            return self.period(at_time=other.at_time, interval=other.interval)

        elif isinstance(other, (BROADCAST, type(BROADCAST))):
            return self.broadcast(eta=other.eta, return_result=other.return_result, timeout=other.timeout)

        else:
            _available_operands = "|".join([o.__name__ for o in (FG, BG, NO_RETURN, PERIOD, BROADCAST)])
            raise GlobalContextError(f"Invalid operand {type(other)}. Use one of {_available_operands}")

    @property
    def info(self) -> Optional["RemoteCallback"]:
        data = search_remote_callback_in_mapping(self._ipc.node.node_callback_mapping, self.func_name)
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
        return self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            eta=eta,
            async_=True,
            return_result=False,
            inside_callback_context=self._inside_callback_context,
        )

    def broadcast(
        self,
        eta: Optional[TimeUnits] = None,
        return_result: Optional[bool] = False,
        timeout: Optional[TimeUnits] = None,
    ) -> Optional[AsyncResult]:
        timeout = self._get_duration(timeout, "timeout")
        eta = self._get_duration(eta, "eta", 0)
        return self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            timeout=timeout,
            eta=eta,
            async_=not return_result,
            return_result=return_result,
            broadcast=True,
            inside_callback_context=self._inside_callback_context,
        )

    def period(
        self, at_time: Optional[Union[List[TimeUnits], TimeUnits]], interval: Optional[TimeUnits]
    ) -> SchedulerTask:
        if at_time is not None:
            if isinstance(at_time, (int, float, str, timedelta, datetime)):
                at_time = [at_time]
            at_time = [self._get_duration(i, "at_time", 0) for i in at_time]
        if interval is not None:
            interval = self._get_duration(interval, "interval", 0)

        func_period = Period(at_time=at_time, interval=interval)
        func_period.validate()
        return self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            async_=True,
            return_result=False,
            func_period=func_period,
            inside_callback_context=self._inside_callback_context,
        )

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

    def __and__(self, other):
        if type(other) == type:
            other = other()
        other_name = other.__class__.__name__
        raise InitializationError(
            f"Invalid syntax. "
            f"You forgot to use parentheses to trigger remote callback."
            f"\nCorrect syntax is "
            f"g.call.{self._func_name}(**args, **kwargs) & {other_name}"
            f" or g.call.{self._func_name}(**args, **kwargs) & {other_name}(**<invocation kwargs>)"
        )

    @property
    def _info(self):
        self._ipc._check_node()
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

    def _wait_function(self) -> NoReturn:
        interval = 0.5

        def condition_executable():
            return self._exist

        self._wait(condition_executable, interval)

    def _wait_process(self, process_name: str) -> NoReturn:
        self._ipc._check_node()
        interval = 0.5

        def condition_executable():
            return process_name in self._ipc.node.node_callback_mapping

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
