from typing import Callable, Any
from daffi.utils.custom_types import P
from daffi.settings import LOCAL_CALLBACK_MAPPING
from daffi.registry.callback import Callback
from daffi.decorators._base import Decorator
from daffi.decorators.fetcher import fetcher


__all__ = ["callback"]


class callback(Decorator):
    """
    callback is decorator for registering remote callbacks from functions.
    Example:
        >>> from daffi.decorators import callback
        >>>
        >>> @callback
        >>> def my_func(*args, **kwargs):
                ...
    """

    def __new__(cls, fn: Callable[P, Any]):
        """
        Custom fetcher initialization when function is wrapped with both `callback` and `fetcher` decorators
        Example:
            >>> from daffi.decorators import fetcher, callback
            >>>
            >>> @fetcher
            >>> @callback
            >>> def my_func(arg1, arg2, argN): ...
        """
        if isinstance(fn, fetcher):
            # If callback and fetcher wraps the same function then fetcher's `proxy` property must be always True.
            # It is requirement since function body belongs to `callback` in this case.
            return fetcher(cls(fn.wrapped), __options=(fn.exec_modifier, True))
        else:
            # Regular `callback` provisioning.
            return super().__new__(cls)

    def __init__(self, fn: Callable[P, Any]):
        self._fn = Callback._init_function(fn)

    def __getattr__(self, item):
        if self._fn:
            return getattr(self._fn, item)
        else:
            try:
                return LOCAL_CALLBACK_MAPPING[item]
            except KeyError:
                ...
