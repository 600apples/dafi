import re
import time
from anyio import to_thread, sleep
from dataclasses import dataclass, field
from datetime import timedelta, datetime
from threading import Event
from inspect import iscoroutine
from typing import Optional, Union, NoReturn, Callable, List, Any, Coroutine

from daffi.async_result import AsyncResult, SchedulerTask
from daffi.exceptions import GlobalContextError, InitializationError
from daffi.utils.timeparse import timeparse
from daffi.utils.custom_types import P, RemoteResult, TimeUnits
from daffi.utils.misc import Period, search_remote_callback_in_mapping
from daffi.execution_modifiers import (
    FG,
    BG,
    NO_RETURN,
    PERIOD,
    BROADCAST,
    STREAM,
    RetryPolicy,
    ALL_EXEC_MODIFIERS,
    is_exec_modifier,
    is_exec_modifier_type,
)

__all__ = ["RemoteCall"]


_empty_result = object()
_available_operands = "|".join([o.__name__ for o in ALL_EXEC_MODIFIERS])


@dataclass
class RemoteCall:
    _ipc: "Ipc" = field(repr=False)
    func_name: Optional[str] = None
    args: P.args = field(default_factory=tuple)
    kwargs: P.kwargs = field(default_factory=dict)
    _inside_callback_context: Optional[bool] = field(repr=False, default=False)
    _result: Optional[Any] = field(repr=False, default=_empty_result)

    # Modify args before requesting remote callback. This callable should return Tuple of arguments.
    _args_pre_processor: Optional[Callable[..., Any]] = field(repr=False, default=None)

    def __str__(self):
        sep = ", "
        args = sep.join(map(str, self.args))
        kwargs = sep.join(f", {k}={v}" for k, v in self.kwargs.items())
        args_kwargs = f"{args}{kwargs}".strip(",").strip()
        return f"<{self.__class__.__name__} {self.func_name}({args_kwargs})>"

    def obtain_result_in_thread(self, next_operand: Union[FG, BG, BROADCAST, NO_RETURN, PERIOD, STREAM]):
        self._result = self & next_operand

    async def __process_await__(
        self, next_operand: Union[FG, BG, BROADCAST, NO_RETURN, PERIOD, STREAM], return_result: Optional[bool] = False
    ):
        if self._args_pre_processor:
            args = self._args_pre_processor(*self.args, **self.kwargs)
            if iscoroutine(args):
                args = await args
            if not isinstance(args, tuple):
                args = (args,)
            self.args = args
            self.kwargs = {}
            self._args_pre_processor = None

        await to_thread.run_sync(self.obtain_result_in_thread, next_operand)
        if return_result:
            return self._result
        return self

    def __and__(self, other) -> RemoteResult:
        if self._result != _empty_result:
            return self._result

        if self._args_pre_processor:
            args = self._args_pre_processor(*self.args, **self.kwargs)
            if iscoroutine(args):
                raise InitializationError("Use await to execute asynchronous fetcher!")
            if not isinstance(args, tuple):
                args = (args,)
            self.args = args
            self.kwargs = {}

        if type(other) == type:
            other = other()

        if is_exec_modifier_type(other, FG):
            return self.fg(timeout=other.timeout, retry_policy=other.retry_policy)

        elif is_exec_modifier_type(other, BG):
            return self.bg(timeout=other.timeout, eta=other.eta)

        elif is_exec_modifier_type(other, NO_RETURN):
            return self.no_return(eta=other.eta)

        elif is_exec_modifier_type(other, PERIOD):
            return self.period(at_time=other.at_time, interval=other.interval)

        elif is_exec_modifier_type(other, BROADCAST):
            return self.broadcast(
                eta=other.eta, return_result=other.return_result, timeout=other.timeout, retry_policy=other.retry_policy
            )

        elif is_exec_modifier_type(other, STREAM):
            return self.stream()

        else:
            GlobalContextError(f"Invalid operand {type(other)}. Use one of {_available_operands}").fire()

    @property
    def info(self) -> Optional["RemoteCallback"]:
        data = search_remote_callback_in_mapping(self._ipc.node_callback_mapping, self.func_name)
        if data:
            return data[1]

    @property
    def exists(self) -> bool:
        return bool(self.info)

    def fg(self, timeout: Optional[TimeUnits] = None, retry_policy: Optional[RetryPolicy] = None) -> RemoteResult:
        executable = retry_policy.wrap(self._ipc.call) if isinstance(retry_policy, RetryPolicy) else self._ipc.call
        timeout = self._get_duration(timeout, "timeout")
        return executable(
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
        retry_policy: Optional[RetryPolicy] = None,
    ) -> Optional[AsyncResult]:
        timeout = self._get_duration(timeout, "timeout")
        eta = self._get_duration(eta, "eta", 0)
        executable = retry_policy.wrap(self._ipc.call) if isinstance(retry_policy, RetryPolicy) else self._ipc.call
        return executable(
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

    def stream(self) -> NoReturn:
        return self._ipc.call(
            self.func_name,
            args=self.args,
            kwargs=None,
            stream=True,
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
                GlobalContextError(
                    f"Invalid {arg_name} format. Should be string, int, float or datetime.timedelta."
                ).fire()

            if val < 0:
                GlobalContextError(f"{arg_name} cannot be negative.").fire()
        return val or default


@dataclass
class LazyRemoteCall:
    _ipc: "Ipc" = field(repr=False)
    _global_terminate_event: Event = field(repr=False)
    _func_name: Optional[str] = field(repr=False, default=None)
    _inside_callback_context: Optional[bool] = field(repr=False, default=False)
    _exec_modifier: "ALL_EXEC_MODIFIERS" = field(repr=False, default=None)
    _is_async: bool = field(repr=False, default=False)

    # In case there is fetcher initialized with 'args_from_body=True'
    _fetcher: Optional[Callable[..., Any]] = field(repr=False, default=None)
    _args_from_body: Optional[bool] = field(repr=False, default=False)

    def __str__(self):
        return self.__class__.__name__ if not self._func_name else f"{self.__class__.__name__}({self._func_name})"

    def __and__(self, other):
        if is_exec_modifier(other):
            self._exec_modifier = other
            return self
        else:
            if type(other) == type:
                other = other()
            other_name = other.__class__.__name__
            InitializationError(
                f"Invalid syntax. "
                f"You forgot to use parentheses to trigger remote callback."
                f"\nCorrect syntax is "
                f"g.call.{self._func_name}(**args, **kwargs) & {other_name}"
                f" or g.call.{self._func_name}(**args, **kwargs) & {other_name}(**<invocation kwargs>)"
            ).fire()

    @property
    def _info(self):
        self._ipc._check_node()
        if self._func_name is None:
            GlobalContextError(
                "Not allowed in current context."
                " Use `Global.call.func_name(*args, **kwargs)` to build RemoteCall instance."
            ).fire()
        elif self._func_name == "":
            GlobalContextError("Empty function name is not allowed.").fire()
        return RemoteCall(_ipc=self._ipc, func_name=self._func_name).info

    @property
    def _exist(self) -> bool:
        return bool(self._info)

    def _wait_function(self, _async: Optional[bool] = False) -> NoReturn:
        interval = 0.5

        def condition_executable():
            return self._exist

        if _async:
            return self._wait_async(condition_executable, interval)
        return self._wait(condition_executable, interval)

    def _wait_process(self, process_name: str, _async: Optional[bool] = False) -> NoReturn:
        self._ipc._check_node()
        interval = 0.5

        def condition_executable():
            return process_name in self._ipc.node_callback_mapping

        if _async:
            return self._wait_async(condition_executable, interval)
        return self._wait(condition_executable, interval)

    def __call__(
        __self__, *args, __exec_modifier__: Optional["ALL_EXEC_MODIFIERS"] = None, **kwargs
    ) -> [RemoteCall, Coroutine]:
        if not __self__._func_name:
            GlobalContextError(
                "Invalid syntax. Use `Global.call.func_name(*args, **kwargs)` to build RemoteCall instance."
            ).fire()

        # Exec modifier passed in arguments is more important then provided in decorator.
        if __exec_modifier__:
            if not is_exec_modifier(__exec_modifier__):
                raise InitializationError(
                    f"Invalid type for `__exec_modifier__` argument. Allowed classes are: {_available_operands}"
                )
            __self__._exec_modifier = __exec_modifier__

        remote_call = RemoteCall(
            _ipc=__self__._ipc,
            func_name=__self__._func_name,
            args=args,
            kwargs=kwargs,
            _args_pre_processor=__self__._fetcher if (__self__._fetcher and __self__._args_from_body) else None,
            _inside_callback_context=__self__._inside_callback_context,
        )
        if __self__._exec_modifier:
            if __self__._is_async:
                return remote_call.__process_await__(next_operand=__self__._exec_modifier, return_result=True)
            return remote_call & __self__._exec_modifier
        return remote_call

    def __getattr__(self, item) -> RemoteCall:
        if self._global_terminate_event and self._global_terminate_event.is_set():
            GlobalContextError("Global can no longer accept remote calls because it was stopped").fire()
        self._func_name = item
        return self

    def _wait(self, condition_executable: Callable, interval: float) -> NoReturn:
        while True:
            if condition_executable():
                break
            time.sleep(interval)

    async def _wait_async(self, condition_executable: Callable, interval: float) -> NoReturn:
        while True:
            if condition_executable():
                break
            await sleep(interval)

    def _set_fetcher_params(self, is_async: bool, fetcher: Optional[Callable[..., Any]], args_from_body: bool):
        self._is_async = is_async
        self._fetcher = fetcher
        self._args_from_body = args_from_body
