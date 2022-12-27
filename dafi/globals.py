import inspect
from copy import copy
from inspect import Signature
from dataclasses import dataclass, field
from functools import cached_property
from inspect import iscoroutinefunction
from threading import Event
from typing import (
    Callable,
    Any,
    Optional,
    NamedTuple,
    Generic,
    List,
    Dict,
    ClassVar,
    Union,
    NoReturn,
    Coroutine,
)

from dafi.backend import LocalBackEnd
from dafi.exceptions import InitializationError, RemoteCallError
from dafi.ipc import Ipc
from dafi.remote_call import LazyRemoteCall
from dafi.utils.misc import Singleton, is_lambda_function, uuid
from dafi.utils.custom_types import GlobalCallback, P
from dafi.utils.func_validation import (
    get_class_methods,
    func_info,
    pretty_callbacks,
    is_class_or_static_method,
)
from dafi.utils.mappings import LOCAL_CALLBACK_MAPPING, LOCAL_CLASS_CALLBACKS

__all__ = ["Global", "callback"]


class RemoteCallback(NamedTuple):
    callback: GlobalCallback
    module: str
    origin_name: str
    signature: Signature
    is_async: bool

    def __call__(self, *args, **kwargs):
        if "g" in self.signature.parameters:
            g = copy(Global._get_self(Global))
            g._inside_callback_context = True
            kwargs["g"] = g
        return self.callback(*args, **kwargs)


class RemoteClassCallback(NamedTuple):
    klass: type
    klass_name: str
    module: str
    origin_name: str
    signature: Signature
    is_async: bool

    @property
    def callback(self):
        if not self.klass:
            raise RemoteCallError(
                f"Instance of {self.klass_name!r} is not initialized yet."
                f" Create instance or mark method {self.origin_name!r}"
                f" as classmethod or staticmethod"
            )
        return getattr(self.klass, self.origin_name)

    def __call__(self, *args, **kwargs):
        if "g" in self.signature.parameters:
            g = copy(Global._get_self(Global))
            g._inside_callback_context = True
            kwargs["g"] = g
        return self.callback(*args, **kwargs)


@dataclass
class Global(metaclass=Singleton):
    process_name: Optional[str] = field(default_factory=uuid)
    init_controller: Optional[bool] = False
    init_node: Optional[bool] = True
    host: Optional[str] = None
    port: Optional[int] = None
    _inside_callback_context: Optional[bool] = field(repr=False, default=False)
    """
    Args:
        process_name: Global process name. If specified it is used as reference key for callback response.
            By default randomly generated hash is used as reference.
    """

    def __post_init__(self):
        if not (self.init_controller or self.init_node):
            raise InitializationError(
                "No components were found in current process."
                " Provide at least one required argument"
                " `init_controller=True` or `init_node=True`."
            )

        if (self.host and not self.port) or (self.port and not self.host):
            raise InitializationError("To work through the TCP socket, the host and port arguments must be defined.")

        self._stop_event = Event()
        self.ipc = Ipc(
            process_name=self.process_name,
            backend=LocalBackEnd(),
            init_controller=self.init_controller,
            init_node=self.init_node,
            stop_event=self._stop_event,
            host=self.host,
            port=self.port,
        )

        callback._ipc = self.ipc
        self.ipc.start()
        self.ipc.wait()

    @cached_property
    def call(self) -> LazyRemoteCall:
        return LazyRemoteCall(
            _ipc=self.ipc, _stop_event=self._stop_event, _inside_callback_context=self._inside_callback_context
        )

    @property
    def is_controller(self) -> bool:
        return bool(self.ipc.controller)

    @property
    def registered_callbacks(self) -> Dict[str, List[str]]:
        return pretty_callbacks(exclude_proc=self.process_name, format="dict")

    def join(self, timeout: Optional[Union[int, float]] = None) -> NoReturn:
        """
        Join global to main thread.
        Don't use this method if you're running asynchronous application as it blocks event loop.
        """
        self.ipc.join(timeout=timeout)

    def stop(self):
        self.ipc.stop()

    def transfer_and_call(
        self, remote_process: str, func: Callable[..., Any], *args, **kwargs
    ) -> Union[Coroutine, Any]:
        if self._inside_callback_context:
            return self.ipc.async_transfer_and_call(remote_process, func, *args, **kwargs)
        return self.ipc.transfer_and_call(remote_process, func, *args, **kwargs)

    @staticmethod
    def wait_function(func_name: str) -> Union[NoReturn, Coroutine]:
        return LazyRemoteCall(_ipc=None, _stop_event=None, _func_name=func_name)._wait_function()

    @staticmethod
    def wait_process(process_name: str) -> Union[NoReturn, Coroutine]:
        return LazyRemoteCall(_ipc=None, _stop_event=None)._wait_process(process_name)


class callback(Generic[GlobalCallback]):
    _ipc: ClassVar[Ipc] = None

    def __init__(self, fn: Callable[P, Any]):

        self._klass = self._fn = None
        if isinstance(fn, type):
            # Class wrapped
            self._klass = fn
            for method in get_class_methods(fn):
                module, name = func_info(method)
                if name.startswith("_"):
                    continue

                cb = RemoteClassCallback(
                    klass=fn if is_class_or_static_method(fn, name) else None,
                    klass_name=fn.__name__,
                    module=module,
                    origin_name=name,
                    signature=inspect.signature(method),
                    is_async=iscoroutinefunction(method),
                )
                LOCAL_CALLBACK_MAPPING[name] = cb
                if self._ipc and self._ipc.is_running:
                    # Update remote callbacks if ips is running. It means callback was not registered during handshake
                    # or callback was added dynamically.
                    self._ipc.update_callbacks({name: cb})

        elif callable(fn):
            if is_lambda_function(fn):
                raise InitializationError("Lambdas is not supported.")

            module, name = func_info(fn)
            self._fn = RemoteCallback(
                callback=fn,
                module=module,
                origin_name=name,
                signature=inspect.signature(fn),
                is_async=iscoroutinefunction(fn),
            )
            LOCAL_CALLBACK_MAPPING[name] = self._fn
            if self._ipc and self._ipc.is_running:
                # Update remote callbacks if ips is running. It means callback was not registered during handshake
                # or callback was added dynamically.
                self._ipc.update_callbacks({name: self._fn})

        else:
            raise InitializationError(f"Invalid type. Provide class or function.")

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
            raise InitializationError(f"Only one callback instance of {class_name!r} should be created.")

        LOCAL_CLASS_CALLBACKS.add(self._klass.__name__)
        self._klass = self._klass(*args, **kwargs)
        for method in get_class_methods(self._klass):
            module, name = func_info(method)
            if name.startswith("_"):
                continue

            info = LOCAL_CALLBACK_MAPPING.get(name)
            if info:
                LOCAL_CALLBACK_MAPPING[name] = info._replace(klass=self._klass)
        return self


# ----------------------------------------------------------------------------------------------------------------------
# Well known callbacks
# ----------------------------------------------------------------------------------------------------------------------


@callback
def __transfer_and_call(func: Callable[..., Any], *args, **kwargs) -> Any:
    return func(*args, **kwargs)


@callback
async def __async_transfer_and_call(func: Callable[..., Any], *args, **kwargs) -> Any:
    return await func(*args, **kwargs)
