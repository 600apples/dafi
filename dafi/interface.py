import os
from abc import ABC, abstractmethod
from typing import Any, NoReturn, Optional


class BackEnd(ABC):

    @property
    @abstractmethod
    def base_dir(self) -> os.PathLike: ...

    @abstractmethod
    def read(self, key: str, default: Optional[Any] = None) -> Any: ...

    @abstractmethod
    def write(self, key: str, value: Any) -> NoReturn: ...

    @abstractmethod
    def write_if_not_exist(self, key: str, value: Any) -> NoReturn: ...

    @abstractmethod
    def delete_key(self, key: str) -> NoReturn: ...


class Master(ABC):

    @abstractmethod
    async def handle(self) -> NoReturn: ...


class Node(ABC):

    @abstractmethod
    async def handle(self) -> NoReturn: ...
