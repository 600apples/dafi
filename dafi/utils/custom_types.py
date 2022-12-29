from datetime import datetime, timedelta
from typing import TypeVar, Union
from typing_extensions import ParamSpec

P = ParamSpec("P")
K = TypeVar("K", bound=str)
GlobalCallback = TypeVar("GlobalCallback")
RemoteResult = TypeVar("RemoteResult")

TimeUnits = Union[int, float, str, datetime, timedelta]
