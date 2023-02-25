from abc import ABCMeta, abstractmethod
from typing import Any, Type, Tuple, Dict, ClassVar

from daffi.ipc import Ipc
from daffi.utils import colors
from daffi.utils.logger import get_daffi_logger

logger = get_daffi_logger("registry", colors.blue)


_base_class_defined = False


class RegistryMeta(ABCMeta):
    def __new__(mcs, cls_name: str, bases: Tuple[Type[Any], ...], namespace: Dict[str, Any], **kwargs: Any) -> Type:
        if _base_class_defined:
            is_registry_class = cls_name in ("BaseRegistry", "Callback")
            auto_init = None
            if not is_registry_class:
                if (auto_init := namespace.get("auto_init", None)) is None:
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
            # this is the BaseRegistry class itself being created, no logic required
            return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


_base_class_defined = True


class BaseRegistry(metaclass=RegistryMeta):
    __slots__ = "__dict__"
    __doc__ = ""  # Null out the Representation docstring

    auto_init: ClassVar[bool] = True
    _ipc: ClassVar[Ipc] = None

    @classmethod
    @abstractmethod
    def _init_class(cls, instance_or_type):
        raise NotImplementedError()

    @classmethod
    def _update_callbacks(cls):
        if cls._ipc and cls._ipc.is_running:
            # Update remote callbacks if ips is running. It means callback was not registered during handshake
            # or callback was added dynamically.
            cls._ipc.update_callbacks()
