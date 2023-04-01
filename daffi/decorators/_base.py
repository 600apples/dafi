from abc import abstractmethod
from typing import (
    Callable,
    Any,
    Union,
    Generic,
)
from daffi.utils.custom_types import GlobalCallback, P, RemoteResult


class Decorator(Generic[GlobalCallback]):

    _fn: GlobalCallback = None
    _store: dict

    @abstractmethod
    def __init__(self, fn: Callable[P, Any]):
        ...  # no cov

    __call__: GlobalCallback
    __getattr__: Union[RemoteResult, Any]

    def __str__(self):
        return f"{self.alias}<{self.__class__.__name__.capitalize()}>"

    __repr__ = __str__

    def __call__(self, *args, **kwargs) -> object:
        return self._fn(*args, **kwargs)

    def __getattr__(self, item):
        if self._fn:
            return getattr(self._fn, item)
        return super().__getattribute__(item)

    @property
    def wrapped(self):
        """Return original function."""
        return self._fn.wrapped

    @property
    def alias(self):
        return self._fn.alias

    @alias.setter
    def alias(self, val):
        """Assign new value for alias."""
        self._fn.alias = val
        # Take updated namedtuple instance from store
        self._fn = self._store[val]
