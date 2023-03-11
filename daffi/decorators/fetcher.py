from functools import partial
from typing import (
    Callable,
    Any,
    Union,
    Optional,
)
from daffi.utils.custom_types import P
from daffi.execution_modifiers import is_exec_modifier, FG, BG, BROADCAST, STREAM, PERIOD, ALL_EXEC_MODIFIERS
from daffi.registry.fetcher import Fetcher
from daffi.decorators._base import Decorator
from daffi.exceptions import InitializationError
from daffi.settings import LOCAL_FETCHER_MAPPING


__all__ = ["fetcher", "__body_unknown__"]


class fetcher(Decorator):
    """
    fetcher is decorator that converts a decorated method into a remote object call.
    Lets consider you have function with name and signature `my_awersome_function(a: int, b: int, c: str): ...`
    registered on one of nodes.
    Yo need to create fetcher with the same name and the same signature to call function on remote:
    Example:
        >>> from daffi.decorators import fetcher, __body_unknown__
        >>>
        >>> @fetcher
        >>> def my_awersome_function(a: int, b: int, c: str):
        >>>     # or use pass. Internal logic will be skipped in any case. only name and signature is important
        >>>     __body_unknown__(a, b, c)
        >>>
        >>> # Then we can call `my_awersome_function` on remote
        >>> # !!! Execution modifier is binded to fetcher. No need to use `& FG` after execution.
        >>> result = my_awersome_function(1, 2, "abc")
    """

    def __new__(
        cls,
        exec_modifier: Union[Callable[P, Any], Union[FG, BG, BROADCAST, STREAM, PERIOD]] = None,
        proxy: Optional[bool] = None,
        **kwargs,
    ):
        if is_exec_modifier(exec_modifier) or proxy is not None:
            return partial(cls, __options=(exec_modifier, proxy))
        return super().__new__(cls)

    def __init__(
        self, exec_modifier: Optional[Union[FG, BG, BROADCAST, STREAM, PERIOD]], proxy: Optional[bool] = True, **kwargs
    ):
        fn = exec_modifier
        exec_modifier, proxy = kwargs.get("__options", (None, proxy))
        if proxy is None:
            proxy = True
        exec_modifier = exec_modifier or FG
        # First argument has name exec_modifier for compatibility between two types of decorator execution
        # @fetcher and @fetcher(exec_modifier=BG) but in fact `exec_modifier` here is always callable.
        self._fn = Fetcher._init_function(fn=fn, proxy=proxy, exec_modifier=exec_modifier)

    @property
    def exec_modifier(self):
        return self._fn.exec_modifier

    @exec_modifier.setter
    def exec_modifier(self, val):
        """Assign new value for exec modifier."""
        if not is_exec_modifier(val):
            raise InitializationError(f"Invalid execution modifiers. Valid modifiers = {ALL_EXEC_MODIFIERS}")
        self._fn.exec_modifier = val
        # Take updated namedtuple instance from LOCAL_FETCHER_MAPPING store
        self._fn = LOCAL_FETCHER_MAPPING[f"{id(self.wrapped)}-{self.origin_name}"]

    @property
    def proxy(self):
        return self._fn.proxy

    @proxy.setter
    def proxy(self, val):
        """Assign new value for proxy."""
        self._fn.proxy = val
        # Take updated namedtuple instance from LOCAL_FETCHER_MAPPING store
        self._fn = LOCAL_FETCHER_MAPPING[f"{id(self.wrapped)}-{self.origin_name}"]


class __body_unknown__:
    def __init__(self, *args, **kwargs):
        """Used to simulate function/method logic"""
        pass

    def __setstate__(self, state):
        return None

    def __getstate__(self):
        return None
