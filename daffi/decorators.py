import logging
from abc import abstractmethod
from cached_property import cached_property
from inspect import iscoroutinefunction, signature
from typing import (
    Callable,
    Any,
    Union,
    Generic,
    ClassVar,
)

from daffi.utils import colors
from daffi.utils.logger import patch_logger
from daffi.exceptions import InitializationError

from daffi.ipc import Ipc
from daffi.utils.misc import Singleton
from daffi.utils.misc import is_lambda_function
from daffi.utils.custom_types import GlobalCallback, P, RemoteResult
from daffi.callback_types import RemoteClassCallback, RemoteCallback
from daffi.utils.func_validation import (
    get_class_methods,
    func_info,
    is_class_or_static_method,
)
from daffi.utils.settings import (
    LOCAL_CALLBACK_MAPPING,
    LOCAL_CLASS_CALLBACKS,
    WELL_KNOWN_CALLBACKS,
)

logger = patch_logger(logging.getLogger(__name__), colors.grey)

__all__ = ["callback", "fetcher", "callback_and_fetcher"]


class Decorator(Generic[GlobalCallback]):
    @abstractmethod
    def __init__(self, fn: Callable[P, Any]):
        ...  # no cov

    __call__: GlobalCallback
    __getattr__: Union[RemoteResult, Any]


class callback(Decorator):
    """
    callback is uniform decorator for registering remote callbacks from functions or from classes
    Example:
        >>> from daffi import callback
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

    _ipc: ClassVar[Ipc] = None

    def __new__(cls, fn: Callable[P, Any]):
        if isinstance(fn, fetcher):
            fn = fn._fn or fn._klass
            return callback_and_fetcher(fn)
        else:
            return super().__new__(cls)

    def __init__(self, fn: Callable[P, Any]):

        self._klass = self._fn = None
        if isinstance(fn, type):
            # Class wrapped
            self._klass = fn
            for method in get_class_methods(fn):
                _, name = func_info(method)

                if name.startswith("_"):
                    continue

                fn_type = is_class_or_static_method(fn, name)
                klass = fn if fn_type else None
                cb = RemoteClassCallback(
                    klass=klass,
                    klass_name=fn.__name__,
                    origin_name=name,
                    signature=signature(method),
                    is_async=iscoroutinefunction(method),
                    is_static=str(fn_type) == "static",
                )
                cb.validate_g_position_type()
                LOCAL_CALLBACK_MAPPING[name] = cb
                logger.info(f"{name!r} registered" + ("" if klass else f" (required {fn.__name__} initialization)"))

                if self._ipc and self._ipc.is_running:
                    # Update remote callbacks if ips is running. It means callback was not registered during handshake
                    # or callback was added dynamically.
                    self._ipc.update_callbacks()

        elif callable(fn):
            if is_lambda_function(fn):
                InitializationError("Lambdas is not supported.").fire()

            _, name = func_info(fn)
            self._fn = RemoteCallback(
                callback=fn,
                origin_name=name,
                signature=signature(fn),
                is_async=iscoroutinefunction(fn),
            )
            self._fn.validate_g_position_type()
            LOCAL_CALLBACK_MAPPING[name] = self._fn
            if name not in WELL_KNOWN_CALLBACKS:
                logger.info(f"{name!r} registered")
            if self._ipc and self._ipc.is_running:
                # Update remote callbacks if ips is running. It means callback was not registered during handshake
                # or callback was added dynamically.
                self._ipc.update_callbacks()

        else:
            InitializationError(f"Invalid type. Provide class or function.").fire()

    def __call__(self, *args, **kwargs) -> object:
        if self._klass:
            return self._build_class_callback_instance(*args, **kwargs)
        return self._fn(*args, **kwargs)

    def __getattr__(self, item):
        if self._fn:
            return getattr(self._fn, item)
        else:
            try:
                return LOCAL_CALLBACK_MAPPING[item]
            except KeyError:
                return getattr(self._klass, item)

    def _build_class_callback_instance(self, *args, **kwargs):
        if isinstance(self._klass, type):
            class_name = self._klass.__name__
        else:
            class_name = self._klass.__class__.__name__
        if class_name in LOCAL_CLASS_CALLBACKS:
            InitializationError(f"Only one callback instance of {class_name!r} should be created.").fire()

        LOCAL_CLASS_CALLBACKS.add(self._klass.__name__)
        self._klass = self._klass(*args, **kwargs)

        method_initialized = False
        for method in get_class_methods(self._klass):
            module, name = func_info(method)
            if name.startswith("_"):
                continue

            info = LOCAL_CALLBACK_MAPPING.get(name)
            if info:
                method_initialized = True
                LOCAL_CALLBACK_MAPPING[name] = info._replace(klass=self._klass)

        if method_initialized and self._ipc and self._ipc.is_running:
            # Update remote callbacks if ips is running. It means callback was not registered during handshake
            # or callback was added dynamically.
            self._ipc.update_callbacks()
        return self


class fetcher(Decorator):
    """
    fetcher is decorator converts a decorated method or class into a remote object call.
    Decorated function cannot be used for local execution.
    Essentially, the 'fetcher' decorator is
    an alternative syntax to `g.call.<remote callback>(*args, **kwargs) * < exec modifier>.
    which is more friendly to IDE.

    Lets consider you have function with name and signature `my_awersome_function(a: int, b: int, c: str): ...`
    registered on one of nodes.
    Yo need to create fetcher with the same name and the same signature to call function on remote:

    Example:

        >>>
        >>> from daffi import fetcher, __signature_unknown__, FG
        >>>
        >>> @fetcher
        >>> def my_awersome_function(a: int, b: int, c: str):
        >>>     # or use pass. Internal logic will be skipped in any case. only name and signature is important
        >>>     __signature_unknown__(a, b, c)
        >>>
        >>> # Then we can call `my_awersome_function` on remote
        >>> result = my_awersome_function(1, 2, "abc") & FG
    """

    def __init__(self, fn: Callable[P, Any]):
        self._klass = self._fn = None

        if isinstance(fn, callback):
            if fn._fn:
                self._fn = fn._fn.callback
            else:
                self._klass = fn

        elif isinstance(fn, type):
            # Class wrapped
            self._klass = fn

        elif callable(fn):
            if is_lambda_function(fn):
                InitializationError("Lambdas is not supported.").fire()

            self._fn = fn

    @cached_property
    def _g(self) -> "Global":
        return Singleton._get_self("Global")

    def __call__(self, *args, **kwargs):
        if self._fn:
            _, name = func_info(self._fn)
            return getattr(self._g.call, name)(*args, **kwargs)

        elif self._klass:
            self._klass = self._klass(*args, **kwargs)
            return self

        else:
            raise NotImplementedError

    def __getattr__(self, item):

        if self._fn:
            return getattr(self._fn, item)

        elif self._klass:
            if str(item).startswith("_"):
                return getattr(self._klass, item)
            return getattr(self._g.call, item)


class __signature_unknown__:
    def __init__(self, *args, **kwargs):
        """Used to simulate function/method logic"""
        pass

    def __setstate__(self, state):
        return None

    def __getstate__(self):
        return None


def callback_and_fetcher(fn: Callable[P, Any]):
    return fetcher(callback(fn))
