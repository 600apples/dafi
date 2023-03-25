from inspect import Signature
from typing import NamedTuple, Union

import daffi
from daffi.utils.misc import Singleton
from daffi.exceptions import GlobalContextError
from daffi.utils.custom_types import GlobalCallback, P, RemoteResult
from daffi.execution_modifiers import FG, BG, BROADCAST, PERIOD
from daffi.settings import LOCAL_FETCHER_MAPPING, LOCAL_CALLBACK_MAPPING

__all__ = ["CallbackExecutor", "ClassCallbackExecutor", "FetcherExecutor", "ClassFetcherExecutor"]

# ----------------------------------------------------------------------------------------------------------------------
# Callback types
# ----------------------------------------------------------------------------------------------------------------------


def c__call__(self, *args, **kwargs) -> RemoteResult:
    """Execute callback"""
    if hasattr((executable := self.wrapped), "origin_method"):
        # In case method is used by fetcher and callback
        executable = executable.origin_method
    return executable(*args, **kwargs)


@property
def alias(self):
    return self.origin_name_


@alias.setter
def alias(self, val):
    """
    Assign new origin_name value to executor.
    As namedtuple is immutable we need to replace current instance and update LOCAL_CALLBACK_MAPPING reference.
    """
    for k, v in LOCAL_CALLBACK_MAPPING.items():
        if v is self:
            break
    else:
        raise GlobalContextError(f"Instance of callback not found.")
    LOCAL_CALLBACK_MAPPING[val] = self._replace(origin_name_=val)
    del LOCAL_CALLBACK_MAPPING[k]


class CallbackExecutor(NamedTuple):
    # Immutable attributes
    wrapped: GlobalCallback
    signature: Signature
    is_async: bool
    is_generator: bool

    # Mutable attributes (can be re-assigned via setter)
    origin_name_: str

    __call__ = c__call__
    alias = alias

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
            e.args += (
                f"Function signature: def {self.alias}{self.signature}: ...,"
                f" provided arguments: args={args}, kwargs={kwargs}",
            )
            GlobalContextError("\n".join(e.args)).fire()


class ClassCallbackExecutor(NamedTuple):
    # Immutable attributes
    klass: type
    signature: Signature
    is_async: bool
    is_static: bool
    is_generator: bool

    # Mutable attributes (can be re-assigned via setter)
    origin_name_: str

    __call__ = c__call__
    alias = alias

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
                f" Create instance or mark method {self.alias!r}"
                f" as classmethod or staticmethod"
            ).fire()
        return getattr(self.klass, self.alias)

    def validate_provided_arguments(self, *args, **kwargs):
        try:
            exec(
                f"class _:\n\tdef _{self.signature}: pass\n_()._(*args, **kwargs)",
                {"args": args, "kwargs": kwargs, "daffi": daffi},
            )
        except TypeError as e:
            e.args += (
                f"Function signature: def {self.alias}{self.signature}: ...,"
                f" provided arguments: args={args}, kwargs={kwargs}",
            )
            GlobalContextError("\n".join(e.args)).fire()


# ----------------------------------------------------------------------------------------------------------------------
# Fetcher types
# ----------------------------------------------------------------------------------------------------------------------


def f__call__(self, *args, **kwargs) -> RemoteResult:
    """Trigger executor with assigned execution modifier."""
    # Get remote call by name
    remote_call = getattr(_g().call, self.alias)
    # Set fetcher
    remote_call._fetcher = self
    # Execute remote call
    return (remote_call & self.exec_modifier)(*args, **kwargs)


def call(self, *args, exec_modifier: Union[FG, BG, BROADCAST, PERIOD] = FG, **kwargs):
    """Trigger executor with execution modifier provided in arguments."""
    # Get remote call by name
    remote_call = getattr(_g().call, self.alias)
    # Set fetcher
    remote_call._fetcher = self
    # Execute remote call
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


@property
def alias(self):
    return self.origin_name_


@alias.setter
def alias(self, val):
    """
    Assign new origin_name value to executor.
    As namedtuple is immutable we need to replace current instance and update LOCAL_FETCHER_MAPPING reference.
    """
    for k, v in LOCAL_FETCHER_MAPPING.items():
        if v is self:
            break
    else:
        raise GlobalContextError(f"Instance of fetcher not found.")
    LOCAL_FETCHER_MAPPING[val] = self._replace(origin_name_=val)
    del LOCAL_FETCHER_MAPPING[k]


class FetcherExecutor(NamedTuple):
    # Immutable attributes
    wrapped: GlobalCallback
    is_async: bool
    is_generator: bool

    # Mutable attributes (can be re-assigned via setter)
    origin_name_: str
    proxy_: bool
    exec_modifier_: Union[FG, BG, BROADCAST, PERIOD]

    __call__ = f__call__
    call = call
    proxy = proxy
    exec_modifier = exec_modifier
    alias = alias


class ClassFetcherExecutor(NamedTuple):
    # Immutable attributes
    klass: type
    origin_method: GlobalCallback
    is_async: bool
    is_static: bool
    is_generator: bool

    # Mutable attributes (can be re-assigned via setter)
    origin_name_: str
    proxy_: bool
    exec_modifier_: Union[FG, BG, BROADCAST, PERIOD]

    __call__ = f__call__
    call = call
    proxy = proxy
    exec_modifier = exec_modifier
    alias = alias

    @property
    def wrapped(self) -> GlobalCallback:
        if not self.klass:
            GlobalContextError(
                f"Instance is not initialized yet."
                f" Create instance or mark method {self.alias!r}"
                f" as classmethod or staticmethod"
            ).fire()
        return self.origin_method
