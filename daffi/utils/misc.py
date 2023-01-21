import types
import asyncio
from uuid import uuid4
from random import choice
from functools import partial
from datetime import datetime
from queue import Queue
from contextlib import contextmanager
from collections.abc import Iterable
from typing import Callable, Any, NamedTuple, NoReturn, Dict, DefaultDict, Optional, Tuple, Union, Sequence, List

import sniffio
from anyio import to_thread, Condition, move_on_after, CancelScope, run
from daffi.utils.custom_types import SchedulerTaskType
from daffi.exceptions import InitializationError
from daffi.utils.custom_types import GlobalCallback, K, AcceptableErrors


class Period(NamedTuple):
    at_time: Optional[List[int]] = None
    interval: Optional[int] = None

    def validate(self):

        if self.at_time is None and self.interval is None:
            InitializationError(
                "Provide one of 'at_time' argument or 'interval' argument during Period initialization"
            ).fire()

        if self.at_time is not None and self.interval is not None:
            InitializationError("Only 1 time unit is allowed. Provide either 'at_time' or 'interval' argument").fire()

        if self.at_time is not None:
            now = datetime.utcnow().timestamp()
            if len(self.at_time) > 1000:
                InitializationError("Too many scheduled at time periods. Provide no more then 1000 timestamps.").fire()
            if any(i <= now for i in self.at_time):
                InitializationError(
                    "One or mote timestamps in 'at_time' argument "
                    "are less then current timestamp. "
                    "Make sure you pass timestamps that are greater then current time"
                ).fire()

        if self.interval is not None and self.interval <= 0:
            InitializationError(
                "Provided 'period' timestamp is less then 0."
                " Make sure you pass period that allows scheduler to execute task periodically"
            ).fire()

    @property
    def scheduler_type(self) -> SchedulerTaskType:
        if self.at_time:
            return "at_time"
        else:
            return "interval"


class ReconnectFreq:
    LIMIT = 15

    def __init__(self, reconnect_freq: int):
        self.reconnect_freq = reconnect_freq
        self._limited = False
        self._locked = False

    def __bool__(self):
        return self.reconnect_freq is not None

    @property
    def value(self):
        if self._limited:
            return self.LIMIT
        return self.reconnect_freq

    @property
    def locked(self):
        return self._locked

    @contextmanager
    def locker(self):
        try:
            self.lock()
            yield
        finally:
            self.unlock()

    def lock(self):
        self._locked = True

    def unlock(self):
        self._locked = False

    def limit_freq(self):
        self._limited = True

    def restore_freq(self):
        self._limited = False


class ConditionObserver:
    def __init__(self, condition_timeout: int):
        self.condition = Condition()
        self.condition_timeout = condition_timeout
        self.locked = False

        self._done_callbacks = []
        self._fail_callbacks = []

    def register_done_callback(self, cb: Callable[..., Any], *args, **kwargs) -> NoReturn:
        self._done_callbacks.append((cb, args, kwargs))

    def register_fail_callback(self, cb: Callable[..., Any], *args, **kwargs) -> NoReturn:
        self._fail_callbacks.append((cb, args, kwargs))

    async def done(self):
        async with self.condition:
            self.condition.notify_all()

    async def wait(self):
        async with self.condition:
            await self.condition.wait()

    async def fire(self):
        with CancelScope(shield=True):
            async with self.condition:
                self.locked = True
                with move_on_after(self.condition_timeout) as cancel_scope:
                    await self.condition.wait()
                if cancel_scope.cancel_called:
                    self.condition.notify_all()
                    callbacks = self._fail_callbacks
                else:
                    callbacks = self._done_callbacks
            for cb, args, kwargs in callbacks:
                res = cb(*args, **kwargs)
                if asyncio.iscoroutine(res):
                    await res


class ConditionEvent:
    """Register Event objects and wait for release when any of them is set"""

    def __init__(self):
        self._cond_q = Queue(maxsize=1)
        self._is_success = False

    @property
    def success(self) -> bool:
        return self._is_success

    def mark_success(self) -> NoReturn:
        self._cond_q.put(True)

    def mark_fail(self) -> NoReturn:
        self._cond_q.put(False)

    def wait(self) -> bool:
        self._is_success = self._cond_q.get()
        return self._is_success


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls.__name__ not in cls._instances:
            cls._instances[cls.__name__] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls.__name__]

    @classmethod
    def _get_self(cls, key: object):
        """
        Get instance of Singleton by provided class.
        It works only with those instance which were already instantiated.
        """
        try:
            return cls._instances[key]
        except KeyError:
            InitializationError(f"{key} is not initialized.").fire()


@contextmanager
def resilent(acceptable: AcceptableErrors = Exception):
    """Suppress exceptions raised from the wrapped scope."""
    try:
        yield
    except acceptable:
        ...


def async_library():
    try:
        return sniffio.current_async_library()
    except sniffio.AsyncLibraryNotFoundError:
        ...


def uuid() -> int:
    return uuid4().int & (1 << 32) - 1


def string_uuid() -> str:
    return hex(uuid())


def is_lambda_function(obj):
    return isinstance(obj, types.LambdaType) and obj.__name__ == "<lambda>"


def iterable(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, (str, bytes, dict))


async def run_in_threadpool(func: Callable[..., Any], *args, **kwargs) -> Any:
    if kwargs:  # no cov
        # run_sync doesn't accept 'kwargs', so bind them in here
        func = partial(func, **kwargs)
    return await to_thread.run_sync(func, *args)


async def run_from_working_thread(backend: str, func: Callable[..., Any], *args, **kwargs) -> Any:
    if kwargs:  # no cov
        # run doesn't accept 'kwargs', so bind them in here
        func = partial(func, **kwargs)

    def dec():
        return run(func, *args, backend=backend)

    return await to_thread.run_sync(dec)


def search_remote_callback_in_mapping(
    mapping: DefaultDict[K, Dict[K, GlobalCallback]],
    func_name: str,
    exclude: Optional[Union[str, Sequence]] = None,
    take_all: Optional[bool] = False,
) -> Optional[Union[Tuple[str, "RemoteCallback"], List[Tuple[str, "RemoteCallback"]]]]:

    if isinstance(exclude, str):
        exclude = [exclude]
    exclude = exclude or []
    found = []

    for proc, func_mapping in mapping.items():
        if proc not in exclude:
            remote_callback = func_mapping.get(func_name)
            if remote_callback:
                found.append((proc, remote_callback))
    try:
        if take_all:
            return found
        return choice(found)
    except IndexError:
        ...
