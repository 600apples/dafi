from abc import ABC, abstractmethod
from typing import NoReturn


class ComponentI(ABC):
    @abstractmethod
    async def handle(self, *args, **kwargs) -> NoReturn:
        ...  # no cov

    @abstractmethod
    async def on_init(self) -> NoReturn:
        ...  # no cov

    @abstractmethod
    async def on_stop(self) -> NoReturn:
        ...  # no cov

    @abstractmethod
    async def before_connect(self) -> NoReturn:
        ...  # no cov


class AbstractQueue(ABC):
    """High level interface for component queue"""

    @abstractmethod
    def size(self) -> int:
        ...  # no cov

    @abstractmethod
    def unfinished_tasks(self) -> int:
        ...  # no cov

    @abstractmethod
    def task_done(self) -> NoReturn:
        ...  # no cov

    @abstractmethod
    def reset(self) -> NoReturn:
        ...  # no cov

    @abstractmethod
    def freeze(self, timeout: int) -> NoReturn:
        ...  # no cov

    @abstractmethod
    def proceed(self) -> NoReturn:
        ...  # no cov
