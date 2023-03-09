from abc import abstractmethod
from typing import (
    Callable,
    Any,
    Union,
    Generic,
)
from daffi.utils.custom_types import GlobalCallback, P, RemoteResult


class Decorator(Generic[GlobalCallback]):

    _fn: GlobalCallback

    @abstractmethod
    def __init__(self, fn: Callable[P, Any]):
        ...  # no cov

    __call__: GlobalCallback
    __getattr__: Union[RemoteResult, Any]

    def __call__(self, *args, **kwargs) -> object:
        return self._fn(*args, **kwargs)

    def __getattr__(self, item):
        return getattr(self._fn, item)

    @property
    def wrapped(self):
        """Return original function."""
        return self._fn.wrapped
