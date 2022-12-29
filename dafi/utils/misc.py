import time
import string
import types
import asyncio
from collections import deque
from random import choices
from functools import partial
from typing import Callable, Any, NamedTuple, List, Optional

import sniffio
from anyio import sleep, to_thread

from dafi.utils.custom_types import SchedulerTaskType
from dafi.exceptions import InitializationError


class Period(NamedTuple):
    at_time: Optional[List[int]] = None
    period: Optional[int] = None

    def validate(self):

        if self.at_time is None and self.period is None:
            raise InitializationError(
                "Provide one of 'at_time' argument or 'period' argument during Period initialization"
            )

        if self.at_time is not None and self.period is not None:
            raise InitializationError("Only 1 time unit is allowed. Provide either 'at_time' or 'period' argument")

        if self.at_time is not None:
            now = time.time()
            if len(self.at_time) > 1000:
                raise InitializationError("Too many scheduled at time periods. Provide no more then 1000 timestamps.")
            if any(i <= now for i in self.at_time):
                raise InitializationError(
                    "One or mote timestamps in 'at_time' argument "
                    "are less then current timestamp. "
                    "Make sure you pass timestamps that are greater then current time"
                )

        if self.period is not None and self.period <= 0:
            raise InitializationError(
                "Provided 'period' timestamp is less then 0."
                " Make sure you pass period that allows scheduler to execute task periodically"
            )

    @property
    def scheduler_type(self) -> SchedulerTaskType:
        if self.at_time:
            return "at_time"
        else:
            return "period"


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    @classmethod
    def _get_self(cls, key: object):
        """
        Get instance of Singleton by provided class.
        It works only with those instance which were already instantiated.
        """
        try:
            return cls._instances[key]
        except KeyError:
            raise KeyError(f"{key} is not initialized.")


class AsyncDeque(deque):
    async def get(self):
        while True:
            if not self:
                await sleep(0.1)
                continue
            return self.popleft()


def async_library():
    try:
        return sniffio.current_async_library()
    except sniffio.AsyncLibraryNotFoundError:
        ...


def uuid(k: int = 8):
    alphabet = string.ascii_lowercase + string.digits
    return "".join(choices(alphabet, k=k))


def is_lambda_function(obj):
    return isinstance(obj, types.LambdaType) and obj.__name__ == "<lambda>"


def sync_to_async(fn: Callable[..., Any]):
    async def dec(*args, **kwargs):
        result = fn(*args, **kwargs)
        while asyncio.iscoroutine(result):
            result = await result

    return dec


async def run_in_threadpool(func: Callable[..., Any], *args, **kwargs) -> Any:
    if kwargs:  # pragma: no cover
        # run_sync doesn't accept 'kwargs', so bind them in here
        func = partial(func, **kwargs)
    return await to_thread.run_sync(func, *args)
