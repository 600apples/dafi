from typing import Callable, Any
from daffi.utils.custom_types import P
from daffi.settings import LOCAL_CALLBACK_MAPPING
from daffi.registry._callback import Callback
from daffi.decorators._base import Decorator
from daffi.decorators._fetcher import fetcher


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

    _store = LOCAL_CALLBACK_MAPPING

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
            return fetcher(cls(fn.wrapped), __options=(fn.exec_modifier,))
        else:
            # Regular `callback` provisioning.
            return super().__new__(cls)

    def __init__(self, fn: Callable[P, Any]):
        alias = Callback._get_alias(self, fn)
        self._fn = Callback._init_function(fn=fn, fn_name=alias)
