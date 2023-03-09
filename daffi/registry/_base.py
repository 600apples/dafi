from abc import ABCMeta, abstractmethod
from typing import Any, Type, Tuple, Dict, ClassVar, Callable, Optional

from daffi.ipc import Ipc
from daffi.utils import colors
from daffi.utils.logger import get_daffi_logger

logger = get_daffi_logger("registry", colors.blue)


_base_class_defined = False


class RegistryMeta(ABCMeta):
    def __new__(mcs, cls_name: str, bases: Tuple[Type[Any], ...], namespace: Dict[str, Any], **kwargs: Any) -> Type:
        if _base_class_defined:
            is_registry_class = cls_name in ("BaseRegistry", "Callback", "Fetcher")
            auto_init = None
            if not is_registry_class and (auto_init := namespace.get("auto_init", None)) is None:
                for base in bases:
                    if auto_init := getattr(base, "auto_init", None) is not None:
                        namespace.update(auto_init=auto_init)
                        break

            cls = _type = super().__new__(mcs, cls_name, bases, namespace, **kwargs)  # type: ignore
            if not is_registry_class:
                if auto_init:
                    _type = _type()
                _type._init_class(_type)
            return cls
        else:
            # this is the BaseRegistry class itself being created.
            return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


_base_class_defined = True


class BaseRegistry(metaclass=RegistryMeta):
    __slots__ = "__dict__"
    __doc__ = ""  # Null out the Representation docstring

    _ipc: ClassVar[Ipc] = None

    def __init__(self):
        self._post_init()
        self.__post_init__()

    def __post_init__(self):
        """For additional user specific initialization"""
        pass

    @classmethod
    @abstractmethod
    def _init_class(cls, instance_or_type):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def _init_function(cls, fn: Callable[..., Any], fn_name: Optional[str] = None):
        raise NotImplementedError()

    @abstractmethod
    def _post_init(self):
        raise NotImplementedError()
