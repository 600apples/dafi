from typing import Callable, Any, Optional, Dict, List
from daffi.exceptions import InitializationError

__all__ = ["EXECUTOR_REGISTRY"]


class Executor:

    def __init__(self, func: Callable[..., Any], name: str, cls: Optional[type] = None):
        self.func = func
        self.name = name
        self.cls = cls

    def __str__(self):
        if self.cls:
            if isinstance(self.cls, type):
                cls_name = self.cls.__name__
            else:
                cls_name = self.cls.__class__.__name__
            name = f"{cls_name}.{self.name}"
        else:
            name = self.name
        return f"{name!r}"

    __repr__ = __str__

    def __call__(self, *args, **kwargs):
        if self.cls:
            return getattr(self.cls, self.name)(*args, **kwargs)
        return self.func(*args, **kwargs)


class ExecutorRegistry:
    def __init__(self):
        self.registry: Dict[str, Executor] = dict()
        self.subscribers: List[Callable[[Executor], Any]] = list()

    def __iter__(self):
        return iter(self.registry.items())

    def __bool__(self):
        return bool(self.registry)

    def register(self, name: str, func: Callable[..., Any], cls: Optional[type] = None):
        if existing := self.get(name):
            raise InitializationError(
                f"Callback {existing!r} is already registered. "
                f"Please, use another name or set custom alias to callback."
            )
        self.registry[name] = Executor(func, name, cls)
        for subscriber in self.subscribers:
            subscriber(self.registry[name])

    def get(self, name: str) -> Optional[Executor]:
        return self.registry.get(name)


EXECUTOR_REGISTRY = ExecutorRegistry()
