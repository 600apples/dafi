import logging
from inspect import isclass
from dataclasses import dataclass
from typing import Optional, Union, Tuple, List, Type, Callable, Any
from functools import wraps
from tenacity import retry, retry_if_exception, stop_after_attempt, stop_after_delay, after_log, wait_fixed, stop_any

from daffi.utils import colors
from daffi.utils.logger import patch_logger
from daffi.utils.custom_types import TimeUnits
from daffi.exceptions import InitializationError
from daffi.utils.misc import iterable


__all__ = [
    "FG",
    "BG",
    "PERIOD",
    "BROADCAST",
    "NO_RETURN",
    "RetryPolicy",
    "ALL_EXEC_MODIFIERS",
    "is_exec_modifier",
    "is_exec_modifier_type",
]

logger = patch_logger(logging.getLogger("retry"), colors.grey)


@dataclass
class RetryPolicy:
    acceptable_errors: Union[Type[BaseException], Tuple[Type[BaseException]]]
    stop_after_attempt: Optional[int] = None
    stop_after_delay: Optional[int] = None
    wait: Optional[int] = 5
    str_expression: Optional[str] = None

    def wrap(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        if not self.acceptable_errors:
            raise InitializationError("retry_if_exception_type argument is required")
        if not iterable(self.acceptable_errors):
            self.acceptable_errors = (self.acceptable_errors,)

        _stop = stop_any()
        if self.stop_after_attempt:
            _stop = stop_after_attempt(self.stop_after_attempt)
        elif self.stop_after_delay:
            _stop = stop_after_delay(self.stop_after_delay)

        @retry(
            reraise=True,
            retry=retry_if_exception(self.on_exception),
            after=after_log(logger, logging.WARNING),
            wait=wait_fixed(self.wait),
            stop=_stop,
        )
        @wraps(fn)
        def _dec(*args, **kwargs):
            return fn(*args, **kwargs)

        return _dec

    def on_exception(self, exc: Type[BaseException]) -> bool:
        if type(exc) in self.acceptable_errors:
            if self.str_expression:
                return self.str_expression in str(exc)
            else:
                return True
        return False


@dataclass
class FG:
    timeout: Optional[TimeUnits] = None
    retry_policy: Optional[RetryPolicy] = None


@dataclass
class BG:
    timeout: Optional[TimeUnits] = None
    eta: Optional[TimeUnits] = None


@dataclass
class NO_RETURN:
    eta: Optional[TimeUnits] = None


@dataclass
class PERIOD:
    at_time: Optional[Union[List[TimeUnits], TimeUnits]] = None
    interval: Optional[TimeUnits] = None


@dataclass
class BROADCAST:
    eta: Optional[TimeUnits] = None
    timeout: Optional[TimeUnits] = None  # Works only with return_result=True
    return_result: Optional[bool] = False
    retry_policy: Optional[RetryPolicy] = None


@dataclass
class STREAM:
    ...


ALL_EXEC_MODIFIERS = (FG, BG, NO_RETURN, PERIOD, BROADCAST, STREAM)


def is_exec_modifier(candidate: Union[object, type]) -> bool:
    """Check if provided candidate is instance of one of exec modifiers or class of one of exec modifiers"""
    if isclass(candidate):
        return issubclass(candidate, ALL_EXEC_MODIFIERS)
    return isinstance(candidate, ALL_EXEC_MODIFIERS)


def is_exec_modifier_type(candidate: Union[object, type], exec_modifier: Union["ALL_EXEC_MODIFIERS"]):
    if isclass(candidate):
        return issubclass(candidate, exec_modifier)
    return isinstance(candidate, exec_modifier)
