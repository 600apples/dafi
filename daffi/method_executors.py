from inspect import Signature
from typing import NamedTuple, Union

import daffi
from daffi.utils.misc import Singleton
from daffi.exceptions import GlobalContextError
from daffi.utils.custom_types import GlobalCallback, P, RemoteResult
from daffi.execution_modifiers import FG, BG, STREAM, BROADCAST, PERIOD

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
            e.args += (f"Function signature: def {self.origin_name}{self.signature}",)
            GlobalContextError("\n".join(e.args)).fire()


class ClassCallbackExecutor(NamedTuple):
    klass: type
    klass_name: str
    origin_name: str
    signature: Signature
    is_async: bool
    is_static: bool

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
                f"Instance of {self.klass_name!r} is not initialized yet."
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
            e.args += (
                f"Function signature: def {self.origin_name}{self.signature}. "
                f"Reminder: you should not provide arguments belonging"
                f" to the class or to the instance of the class eg 'self', 'cls' etc.",
            )
            GlobalContextError("\n".join(e.args)).fire()


# ----------------------------------------------------------------------------------------------------------------------
# Fetcher types
# ----------------------------------------------------------------------------------------------------------------------


def f__call__(self, *args, **kwargs) -> RemoteResult:
    remote_call = getattr(_g().call, self.origin_name)
    remote_call._set_fetcher_params(is_async=self.is_async, fetcher=self.wrapped, proxy=self.proxy)
    return (remote_call & self.exec_modifier)(*args, **kwargs)


def _g() -> "Global":
    return Singleton._get_self("Global")


class FetcherExecutor(NamedTuple):
    wrapped: GlobalCallback
    origin_name: str
    is_async: bool
    proxy: bool
    exec_modifier: Union[FG, BG, BROADCAST, STREAM, PERIOD]

    __call__ = f__call__


class ClassFetcherExecutor(NamedTuple):
    klass: type
    origin_name: str
    origin_method: GlobalCallback
    is_async: bool
    is_static: bool
    proxy: bool
    exec_modifier: Union[FG, BG, BROADCAST, STREAM, PERIOD]

    __call__ = f__call__

    @property
    def wrapped(self) -> GlobalCallback:
        if not self.klass:
            GlobalContextError(
                f"Instance of {self.klass_name!r} is not initialized yet."
                f" Create instance or mark method {self.origin_name!r}"
                f" as classmethod or staticmethod"
            ).fire()
        return self.origin_method
