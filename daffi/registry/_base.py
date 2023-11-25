from abc import ABCMeta, abstractmethod
from typing import Any, Type, Tuple, Dict, Callable, Optional, Union, Generic
from daffi.utils.custom_types import GlobalCallback, P, RemoteResult

from daffi.utils import colors
from daffi.utils.logger import get_daffi_logger

logger = get_daffi_logger("registry", colors.blue)

_base_class_defined = False


class RegistryMeta(ABCMeta):
    def __new__(
        mcs,
        cls_name: str,
        bases: Tuple[Type[Any], ...],
        namespace: Dict[str, Any],
        **kwargs: Any,
    ) -> Type:
        if _base_class_defined:
            is_registry_class = cls_name in ("BaseRegistry", "CallbackGroup")
            cls = _type = super().__new__(mcs, cls_name, bases, namespace, **kwargs)  # type: ignore
            if not is_registry_class:
                from daffi import CallbackGroup
                self = _type()
                CallbackGroup._init_class(cls, self)
            return cls
        else:
            # this is the RegistryMeta class itself being created.
            return super().__new__(mcs, cls_name, bases, namespace, **kwargs)  # type: ignore


_base_class_defined = True


class BaseRegistry(metaclass=RegistryMeta):
    __slots__ = "__dict__"
    __doc__ = ""  # Null out the Representation docstring

    @staticmethod
    @abstractmethod
    def _init_class(cls, instance_or_type):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def _init_function(cls, fn: Callable[..., Any], fn_name: Optional[str] = None):
        raise NotImplementedError()


class Decorator(Generic[GlobalCallback]):

    _fn: GlobalCallback = None
    _store: dict

    @abstractmethod
    def __init__(self, fn: Callable[P, Any]):
        ...  # no cov

    __call__: GlobalCallback
    __getattr__: Union[RemoteResult, Any]

    def __str__(self):
        return f"{self.alias}<{self.__class__.__name__.capitalize()}>"

    __repr__ = __str__

    def __call__(self, *args, **kwargs) -> object:
        return self._fn(*args, **kwargs)

    def __getattr__(self, item):
        if self._fn:
            return getattr(self._fn, item)
        return super().__getattribute__(item)

    @property
    def wrapped(self):
        """Return original function."""
        return self._fn.wrapped

    @property
    def alias(self):
        return self._fn.alias

    @alias.setter
    def alias(self, val):
        """Assign new value for alias."""
        self._fn.alias = val
        # Take updated namedtuple instance from store
        self._fn = self._store[val]
