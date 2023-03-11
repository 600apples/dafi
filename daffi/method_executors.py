from inspect import Signature
from typing import NamedTuple, Union

import daffi
from daffi.utils.misc import Singleton
from daffi.exceptions import GlobalContextError
from daffi.utils.custom_types import GlobalCallback, P, RemoteResult
from daffi.execution_modifiers import FG, BG, STREAM, BROADCAST, PERIOD
from daffi.settings import LOCAL_FETCHER_MAPPING

__all__ = ["CallbackExecutor", "ClassCallbackExecutor", "FetcherExecutor", "ClassFetcherExecutor"]

# ----------------------------------------------------------------------------------------------------------------------
# Callback types
# ----------------------------------------------------------------------------------------------------------------------


def c__call__(self, *args, **kwargs) -> RemoteResult:
    return self.wrapped(*args, **kwargs)


class CallbackExecutor(NamedTuple):
    wrapped: GlobalCallback
    origin_name: str
    signature: Signature
    is_async: bool
    is_generator: bool

    __call__ = c__call__

    def simplified(self) -> "CallbackExecutor":
        """
        Remove or convert unnecessary attributes for serialization.
        ! This method makes copy of original callback
          by removing those attributes which can cause serialization errors on remote side
        """
        return self.__class__(
            **{
                **self._asdict(),
                "wrapped": True,
            }
        )

    def validate_provided_arguments(self, *args: P.args, **kwargs: P.kwargs):
        try:
            exec(f"def _{self.signature}: pass\n_(*args, **kwargs)", {"args": args, "kwargs": kwargs, "daffi": daffi})
        except TypeError as e:
            e.args += (f"Function signature: def {self.origin_name}{self.signature}: ...",)
            GlobalContextError("\n".join(e.args)).fire()


class ClassCallbackExecutor(NamedTuple):
    klass: type
    origin_name: str
    signature: Signature
    is_async: bool
    is_static: bool
    is_generator: bool

    __call__ = c__call__

    def simplified(self) -> "ClassCallbackExecutor":
        """
        Remove or convert unnecessary attributes for serialization.
        ! This method makes copy of original callback
          by removing those attributes which can cause serialization errors on remote side
        """
        return self.__class__(
            **{
                **self._asdict(),
                "klass": bool(self.klass),
            }
        )

    @property
    def wrapped(self) -> GlobalCallback:
        if not self.klass:
            GlobalContextError(
                f"Instance is not initialized yet."
                f" Create instance or mark method {self.origin_name!r}"
                f" as classmethod or staticmethod"
            ).fire()
        return getattr(self.klass, self.origin_name)

    def validate_provided_arguments(self, *args, **kwargs):
        try:
            exec(
                f"class _:\n\tdef _{self.signature}: pass\n_()._(*args, **kwargs)",
                {"args": args, "kwargs": kwargs, "daffi": daffi},
            )
        except TypeError as e:
            e.args += (f"Function signature: def {self.origin_name}{self.signature}: ...",)
            GlobalContextError("\n".join(e.args)).fire()


# ----------------------------------------------------------------------------------------------------------------------
# Fetcher types
# ----------------------------------------------------------------------------------------------------------------------


def f__call__(self, *args, **kwargs) -> RemoteResult:
    """Trigger executor with assigned execution modifier."""
    remote_call = getattr(_g().call, self.origin_name)
    remote_call._set_fetcher_params(is_async=self.is_async, fetcher=self.wrapped, proxy=self.proxy)
    return (remote_call & self.exec_modifier)(*args, **kwargs)


def call(self, *args, exec_modifier: Union[FG, BG, BROADCAST, STREAM, PERIOD] = None, **kwargs):
    """Trigger executor with execution modifier provided in arguments."""
    remote_call = getattr(_g().call, self.origin_name)
    remote_call._set_fetcher_params(is_async=self.is_async, fetcher=self.wrapped, proxy=self.proxy)
    return (remote_call & exec_modifier)(*args, **kwargs)


def _g() -> "Global":
    return Singleton._get_self("Global")


@property
def proxy(self):
    return self.proxy_


@proxy.setter
def proxy(self, val):
    """
    Assign new proxy value to executor.
    As namedtuple is immutable we need to replace current instance and update LOCAL_FETCHER_MAPPING reference.
    """
    for k, v in LOCAL_FETCHER_MAPPING.items():
        if v is self:
            break
    else:
        raise GlobalContextError(f"Instance of fetcher not found.")
    LOCAL_FETCHER_MAPPING[k] = self._replace(proxy_=val)


@property
def exec_modifier(self):
    return self.exec_modifier_


@exec_modifier.setter
def exec_modifier(self, val):
    """
    Assign new exec_modifier value to executor.
    As namedtuple is immutable we need to replace current instance and update LOCAL_FETCHER_MAPPING reference.
    """
    for k, v in LOCAL_FETCHER_MAPPING.items():
        if v is self:
            break
    else:
        raise GlobalContextError(f"Instance of fetcher not found.")
    LOCAL_FETCHER_MAPPING[k] = self._replace(exec_modifier_=val)


class FetcherExecutor(NamedTuple):
    # Immutable attributes
    wrapped: GlobalCallback
    origin_name: str
    is_async: bool

    # Mutable attributes (can be re-assigned via setter)
    proxy_: bool
    exec_modifier_: Union[FG, BG, BROADCAST, STREAM, PERIOD]

    __call__ = f__call__
    call = call
    proxy = proxy
    exec_modifier = exec_modifier


class ClassFetcherExecutor(NamedTuple):
    # Immutable attributes
    klass: type
    origin_name: str
    origin_method: GlobalCallback
    is_async: bool
    is_static: bool

    # Mutable attributes (can be re-assigned via setter)
    proxy_: bool
    exec_modifier_: Union[FG, BG, BROADCAST, STREAM, PERIOD]

    __call__ = f__call__
    call = call
    proxy = proxy
    exec_modifier = exec_modifier

    @property
    def wrapped(self) -> GlobalCallback:
        if not self.klass:
            GlobalContextError(
                f"Instance is not initialized yet."
                f" Create instance or mark method {self.origin_name!r}"
                f" as classmethod or staticmethod"
            ).fire()
        return self.origin_method
