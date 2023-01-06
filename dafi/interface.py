from abc import ABC, abstractmethod
from typing import NoReturn


class ComponentI(ABC):
    @abstractmethod
    async def handle(self, *args, **kwargs) -> NoReturn:
        ...

    @abstractmethod
    async def on_init(self) -> NoReturn:
        ...

    @abstractmethod
    async def on_stop(self) -> NoReturn:
        ...

    @abstractmethod
    async def before_connect(self) -> NoReturn:
        ...


class AbstractQueue(ABC):
    """High level interface for component queue"""

    @abstractmethod
    def size(self) -> int:
        ...

    @abstractmethod
    def unfinished_tasks(self) -> int:
        ...

    @abstractmethod
    def task_done(self) -> NoReturn:
        ...

    @abstractmethod
    def reset(self) -> NoReturn:
        ...

    @abstractmethod
    def freeze(self, timeout: int) -> NoReturn:
        ...

    @abstractmethod
    def proceed(self) -> NoReturn:
        ...
