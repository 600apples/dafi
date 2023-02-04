import pickle
import traceback
from logging import Logger
from dataclasses import dataclass, field
from cached_property import cached_property
from types import TracebackType
from typing import Optional, NoReturn, Type


class BaseException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

    def fire(self):
        raise self


class RemoteCallError(BaseException):
    ...


class UnableToFindCandidate(BaseException):
    ...


class RemoteStoppedUnexpectedly(BaseException):
    ...


class InitializationError(BaseException):
    ...


class GlobalContextError(BaseException):
    ...


class TimeoutError(BaseException):
    ...


class ReckAcceptError(Exception):
    ...


class StopComponentError(Exception):
    ...


@dataclass
class RemoteError:
    """It is not Exception itself but container to transfer exceptions from remove executor to caller."""

    info: Optional[str] = None
    traceback: Optional[bytes] = None
    _origin_traceback: TracebackType = field(repr=False, default=None)
    _awaited_error_type: Type[Exception] = field(repr=False, default=RemoteCallError)

    @cached_property
    def unpickled_trackeback(self) -> TracebackType:
        if self._origin_traceback:
            return self._origin_traceback
        elif self.traceback:
            return pickle.loads(self.traceback)[2]

    def show_in_log(self, logger: Logger) -> NoReturn:
        if self.info:
            logger.error(self.info)
        if self.traceback:
            traceback.print_tb(self.unpickled_trackeback)

    def raise_with_trackeback(self):
        if self.unpickled_trackeback:
            RemoteCallError(self.info).with_traceback(self.unpickled_trackeback).fire()
        else:
            err = self._awaited_error_type(self.info)
            if hasattr(err, "fire"):
                err.fire()
            else:
                raise err
