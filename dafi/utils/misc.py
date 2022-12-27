import string
import types
import asyncio
from collections import deque
from random import choices
from typing import Callable, Any

import sniffio
from anyio import sleep


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
