import pickle
import traceback
from logging import Logger
from dataclasses import dataclass, field
from cached_property import cached_property
from types import TracebackType
from typing import Optional, NoReturn, Type


class RemoteCallError(Exception):
    ...


class UnableToFindCandidate(Exception):
    ...


class RemoteStoppedUnexpectedly(Exception):
    ...


class InitializationError(Exception):
    ...


class GlobalContextError(Exception):
    ...


class TimeoutError(Exception):
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
            raise RemoteCallError(self.info).with_traceback(self.unpickled_trackeback)
        else:
            raise self._awaited_error_type(self.info)
