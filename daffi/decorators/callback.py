from typing import (
    Callable,
    Any,
    Union,
)
from daffi.exceptions import InitializationError
from daffi.utils.custom_types import P
from daffi.settings import LOCAL_CALLBACK_MAPPING
from daffi.registry.callback import Callback
from daffi.decorators._base import Decorator
from daffi.decorators.fetcher import fetcher
from daffi.execution_modifiers import FG, BG, BROADCAST, STREAM, PERIOD, is_exec_modifier


__all__ = ["callback", "callback_and_fetcher"]


class callback(Decorator):
    """
    callback is uniform decorator for registering remote callbacks from functions or from classes
    Example:
        >>> from daffi.decorators import callback
        >>>
        >>> @callback
        >>> def my_func(*args, **kwargs):
                ...

    callback works with classes but some limitations are present:

        1. Only `static` methods and `class` methods
            can be triggered without class initialization.
            For all other methods to be instance of class should be instantiated.
        2. Only one instance of class can be instantiated.
        3. Only publicly available methods become callback (means methods which name doensn't start with underscore)
    """

    def __new__(cls, fn: Callable[P, Any]):
        if isinstance(fn, fetcher):
            return callback_and_fetcher(fn._fn)
        else:
            return super().__new__(cls)

    def __init__(self, fn: Callable[P, Any]):
        self._fn = None
        if isinstance(fn, type):
            raise InitializationError("Classes are not supported").fire()

        elif callable(fn):
            self._fn = Callback._init_function(fn)

        else:
            InitializationError(f"Invalid type. Provide class or function.").fire()

    def __getattr__(self, item):
        if self._fn:
            return getattr(self._fn, item)
        else:
            try:
                return LOCAL_CALLBACK_MAPPING[item]
            except KeyError:
                ...


def callback_and_fetcher(exec_modifier: Union[Callable[P, Any], Union[FG, BG, BROADCAST, STREAM, PERIOD]]):
    def _dec(fn: Callable[P, Any]):
        return fetcher(callback(fn), __options=(exec_modifier, None))

    if is_exec_modifier(exec_modifier):
        return _dec

    return fetcher(callback(exec_modifier))
