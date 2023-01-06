from copy import copy
from itertools import chain
from inspect import Parameter, Signature
from typing import (
    Optional,
    NamedTuple,
    Tuple,
    NoReturn,
)

from dafi.exceptions import InitializationError, RemoteCallError, GlobalContextError
from dafi.utils.custom_types import GlobalCallback, P, RemoteResult
from dafi.utils.debug import with_debug_trace
from dafi.utils.misc import Singleton


__all__ = ["RemoteCallError", "RemoteClassCallback"]


def get_g_param(signature: Signature, is_static_method: Optional[bool] = True) -> Optional[Tuple[Parameter, int]]:
    try:
        return next(
            (param, ind if is_static_method else ind - 1)
            for ind, (k, param) in enumerate(signature.parameters.items())
            if k == "g"
        )
    except StopIteration:
        return None, None


@with_debug_trace
def validate_g_position(
    args: P.args, kwargs: P.kwargs, signature: Signature, is_static_method: Optional[bool] = True
) -> Optional[Parameter]:
    error_msg = (
        "It is forbidden to pass the 'g; argument in parameters."
        " The 'g' object will be injected into callback on the receiver side"
        " (or locally if you're using it as regular function)"
    )
    if "g" in kwargs:
        raise GlobalContextError(error_msg)
    g_param, index = get_g_param(signature=signature, is_static_method=is_static_method)
    if g_param and (g_param.kind == Parameter.POSITIONAL_OR_KEYWORD and len(args) > index):
        raise GlobalContextError(error_msg)
    return g_param


def validate_g_presence_in_arguments(*args: P.args, **kwargs: P.kwargs) -> NoReturn:
    g_obj = Singleton._get_self("Global")
    if any(isinstance(arg, g_obj.__class__) for arg in chain(args, kwargs.values())):
        raise GlobalContextError(
            "'g' object cannot be serialized or passed to remote callback as argument. "
            "If you want to have 'g' object inside remote callback you can specify it as 'g' argument."
            " 'g' will be injected in callback automatically during invocation."
        )


def validate_g_position_type(self) -> NoReturn:
    g_param, index = get_g_param(signature=self.signature)
    if g_param and g_param.kind not in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY):
        raise InitializationError(
            f"The 'g' object can be either 'keyword_or_positional' or 'keyword_only' argument. "
            f"Please refactor the {self.origin_name!r} callback so that it takes the correct value of 'g'"
        )


def __call__(self, *args, **kwargs) -> RemoteResult:
    if "g" in self.signature.parameters:
        g = copy(Singleton._get_self("Global"))
        if self.is_async:
            g._inside_callback_context = True
        kwargs["g"] = g
    return self.callback(*args, **kwargs)


class RemoteCallback(NamedTuple):
    callback: GlobalCallback
    origin_name: str
    signature: Signature
    is_async: bool

    __call__ = __call__
    validate_g_position_type = validate_g_position_type

    @with_debug_trace
    def validate_provided_arguments(self, *args: P.args, **kwargs: P.kwargs):
        import dafi

        validate_g_presence_in_arguments(*args, **kwargs)
        if validate_g_position(args=args, kwargs=kwargs, signature=self.signature):
            kwargs["g"] = None
        exec(f"def _{self.signature}: pass")
        try:
            locals()["_"](*args, **kwargs)
        except TypeError as e:
            e.args += (f"Function signature: def {self.origin_name}{self.signature}",)
            raise


class RemoteClassCallback(NamedTuple):
    klass: type
    klass_name: str
    origin_name: str
    signature: Signature
    is_async: bool
    is_static: bool

    __call__ = __call__
    validate_g_position_type = validate_g_position_type

    @property
    def callback(self) -> GlobalCallback:
        if not self.klass:
            raise RemoteCallError(
                f"Instance of {self.klass_name!r} is not initialized yet."
                f" Create instance or mark method {self.origin_name!r}"
                f" as classmethod or staticmethod"
            )
        return getattr(self.klass, self.origin_name)

    def validate_provided_arguments(self, *args, **kwargs):
        import dafi

        validate_g_presence_in_arguments(*args, **kwargs)
        if validate_g_position(args=args, kwargs=kwargs, signature=self.signature, is_static_method=self.is_static):
            kwargs["g"] = None

        exec(f"class _:\n\tdef _{self.signature}: pass")
        try:
            getattr(locals()["_"](), "_")(*args, **kwargs)
        except TypeError as e:
            e.args += (
                f"Function signature: def {self.origin_name}{self.signature}. "
                f"Reminder: you should not provide arguments belonging"
                f" to the class or to the instance of the class eg 'self', 'cls' etc.",
            )
            raise
