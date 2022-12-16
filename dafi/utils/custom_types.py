from typing import TypeVar
from typing_extensions import ParamSpec

P = ParamSpec("P")
K = TypeVar("K", bound=str)
GlobalCallback = TypeVar("GlobalCallback")
RemoteResult = TypeVar("RemoteResult")
