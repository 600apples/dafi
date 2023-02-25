from inspect import Signature
from typing import NamedTuple

import daffi
from daffi.exceptions import RemoteCallError, GlobalContextError
from daffi.utils.custom_types import GlobalCallback, P, RemoteResult

__all__ = ["RemoteCallError", "RemoteClassCallback"]


def __call__(self, *args, **kwargs) -> RemoteResult:
    return self.callback(*args, **kwargs)


class RemoteCallback(NamedTuple):
    callback: GlobalCallback
    origin_name: str
    signature: Signature
    is_async: bool

    __call__ = __call__

    def simplified(self) -> "RemoteCallback":
        """
        Remove or convert unnecessary attributes for serialization.
        ! This method makes copy of original callback
          by removing those attributes which can cause serialization errors on remote side
        """
        return self.__class__(
            **{
                **self._asdict(),
                "callback": True,
            }
        )

    def validate_provided_arguments(self, *args: P.args, **kwargs: P.kwargs):
        try:
            exec(f"def _{self.signature}: pass\n_(*args, **kwargs)", {"args": args, "kwargs": kwargs, "daffi": daffi})
        except TypeError as e:
            e.args += (f"Function signature: def {self.origin_name}{self.signature}",)
            GlobalContextError("\n".join(e.args)).fire()


class RemoteClassCallback(NamedTuple):
    klass: type
    klass_name: str
    origin_name: str
    signature: Signature
    is_async: bool
    is_static: bool

    __call__ = __call__

    def simplified(self) -> "RemoteClassCallback":
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
    def callback(self) -> GlobalCallback:
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
