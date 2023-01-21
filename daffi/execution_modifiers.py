import logging
from dataclasses import dataclass
from typing import Optional, Union, Tuple, List, Type, Callable, Any

from tenacity import retry, retry_if_exception_type, stop_after_attempt, stop_after_delay, after_log, wait_fixed

from daffi.utils import colors
from daffi.utils.logger import patch_logger
from daffi.utils.custom_types import TimeUnits
from daffi.exceptions import InitializationError
from daffi.utils.misc import iterable


__all__ = ["FG", "BG", "PERIOD", "BROADCAST", "NO_RETURN", "RetryPolicy"]

logger = patch_logger(logging.getLogger("retry"), colors.grey)


@dataclass
class RetryPolicy:
    acceptable_errors: Union[Type[BaseException], Tuple[Type[BaseException]]]
    stop_after_attempt: Optional[int] = None
    stop_after_delay: Optional[int] = None
    wait: Optional[int] = 5
    str_expression: Optional[str] = None

    def build(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        if not self.acceptable_errors:
            raise InitializationError("retry_if_exception_type argument is required")
        if not iterable(self.acceptable_errors):
            self.acceptable_errors = (self.acceptable_errors,)

        options = dict(
            reraise=True,
            retry=retry_if_exception_type(self.acceptable_errors),
            after=after_log(logger, logging.WARNING),
            wait=wait_fixed(self.wait),
        )
        if self.stop_after_delay:
            options["stop"] = stop_after_delay(self.stop_after_delay)
        elif self.stop_after_attempt:
            options["stop"] = stop_after_attempt(self.stop_after_attempt)
        return retry(**options)(fn)


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
