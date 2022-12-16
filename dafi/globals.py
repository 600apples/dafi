import inspect
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


class CallbackInfo(NamedTuple):
    callback: GlobalCallback
    module: str
    origin_name: str
    signature: str
    is_async: bool


class ClassCallbackInfo(NamedTuple):
    klass: type
    klass_name: str
    module: str
    origin_name: str
    signature: str
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


@dataclass
class Global(metaclass=Singleton):
    process_name: Optional[str] = field(default_factory=uuid)
    init_controller: Optional[bool] = True
    init_node: Optional[bool] = True
    """
    Args:
        process_name: Global process name. If specified it is used as reference key for callback response.
            By default randomly generated hash is used as reference.
    """

    def __post_init__(self):
        self._stop_event = Event()
        self.ipc = Ipc(
            process_name=self.process_name,
            backend=LocalBackEnd(),
            init_controller=self.init_controller,
            init_node=self.init_node,
            stop_event=self._stop_event,
        )

        callback.ipc = self.ipc
        self.ipc.start()
        self.ipc.wait()

    @cached_property
    def call(self) -> LazyRemoteCall:
        return LazyRemoteCall(_ipc=self.ipc, _stop_event=self._stop_event)

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

    def transfer_and_call(self, remote_process: str, func: Callable[..., Any], *args, **kwargs) -> Any:
        return self.ipc.transfer_and_call(remote_process, func, *args, **kwargs)

    @staticmethod
    def wait_function(func_name: str) -> Union[NoReturn, Coroutine]:
        return LazyRemoteCall(_ipc=None, _stop_event=None, func_name=func_name).wait_function()

    @staticmethod
    def wait_process(process_name: str) -> Union[NoReturn, Coroutine]:
        return LazyRemoteCall(_ipc=None, _stop_event=None).wait_process(process_name)


class callback(Generic[GlobalCallback]):
    ipc: ClassVar[Ipc] = None

    def __init__(self, fn: Callable[P, Any]):

        self._klass = None
        if isinstance(fn, type):
            # Class wrapped
            self._klass = fn
            for method in get_class_methods(fn):
                module, name = func_info(method)
                if name.startswith("_"):
                    continue

                info = ClassCallbackInfo(
                    klass=fn if is_class_or_static_method(fn, name) else None,
                    klass_name=fn.__name__,
                    module=module,
                    origin_name=name,
                    signature=str(inspect.signature(method)),
                    is_async=iscoroutinefunction(method),
                )
                LOCAL_CALLBACK_MAPPING[name] = info
                if self.ipc and self.ipc.is_running:
                    # Update remote callbacks if ips is running. It means callback was not registered during handshake
                    # or callback was added dynamically.
                    self.ipc.update_callbacks({name: info})

            # TODO create not initialized callback info and pass all methods of class there

        elif callable(fn):
            if is_lambda_function(fn):
                raise InitializationError("Lambdas is not supported.")

            module, name = func_info(fn)
            info = CallbackInfo(
                callback=fn,
                module=module,
                origin_name=name,
                signature=str(inspect.signature(fn)),
                is_async=iscoroutinefunction(fn),
            )
            LOCAL_CALLBACK_MAPPING[name] = info
            if self.ipc and self.ipc.is_running:
                # Update remote callbacks if ips is running. It means callback was not registered during handshake
                # or callback was added dynamically.
                self.ipc.update_callbacks({name: info})

        else:
            raise InitializationError(f"Invalid type. Provide class or function.")

    def __call__(self, *args, **kwargs) -> object:
        if self._klass.__name__ in LOCAL_CLASS_CALLBACKS:
            raise InitializationError(f"Only one callback instance of {self._klass.__name__!r} should be created.")

        LOCAL_CLASS_CALLBACKS.add(self._klass.__name__)
        obj = self._klass(*args, **kwargs)
        for method in get_class_methods(obj):
            module, name = func_info(method)
            if name.startswith("_"):
                continue

            info = LOCAL_CALLBACK_MAPPING.get(name)
            if info:
                LOCAL_CALLBACK_MAPPING[name] = info._replace(klass=obj)
        return obj


# ----------------------------------------------------------------------------------------------------------------------
# Well known callbacks
# ----------------------------------------------------------------------------------------------------------------------


@callback
def __transfer_and_call(func: Callable[..., Any], *args, **kwargs) -> Any:
    return func(*args, **kwargs)


@callback
async def __async_transfer_and_call(func: Callable[..., Any], *args, **kwargs) -> Any:
    return await func(*args, **kwargs)
