from functools import partial
from typing import (
    Callable,
    Any,
    Union,
    Optional,
)
from daffi.utils.custom_types import P
from daffi.execution_modifiers import is_exec_modifier, FG, BG, BROADCAST, STREAM, PERIOD
from daffi.registry.fetcher import Fetcher
from daffi.decorators._base import Decorator


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
        proxy: Optional[bool] = True,
        **kwargs,
    ):
        if is_exec_modifier(exec_modifier) or proxy:
            return partial(cls, __options=(exec_modifier, proxy))
        return super().__new__(cls)

    def __init__(
        self, exec_modifier: Optional[Union[FG, BG, BROADCAST, STREAM, PERIOD]], proxy: Optional[bool] = True, **kwargs
    ):
        options = kwargs.get("__options", (None, proxy))
        self.exec_modifier, self.proxy = options
        self.exec_modifier = self.exec_modifier or FG

        # First argument has name exec_modifier for compatibility between two types of decorator execution
        # @fetcher and @fetcher(exec_modifier=BG) but in fact `exec_modifier` here is always callable.
        self._fn = Fetcher._init_function(fn=exec_modifier, proxy=self.proxy, exec_modifier=self.exec_modifier)


class __body_unknown__:
    def __init__(self, *args, **kwargs):
        """Used to simulate function/method logic"""
        pass

    def __setstate__(self, state):
        return None

    def __getstate__(self):
        return None
