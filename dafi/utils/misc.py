import string
import types
from collections import deque
from contextlib import contextmanager
from random import choices
from typing import Optional, Type, Sequence, Union

import sniffio
from anyio import sleep


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AsyncDeque(deque):
    async def iterate(self):
        while True:
            if not self:
                await sleep(0.1)
                continue
            yield self.popleft()


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


@contextmanager
def resilent(acceptable: Optional[Union[Type[BaseException], Sequence[Type[BaseException]]]] = Exception):
    """Suppress exceptions raised from the wrapped scope."""
    try:
        yield
    except acceptable:
        ...
