from abc import ABCMeta, abstractmethod
from typing import Any, Type, Tuple, Dict, ClassVar, Callable, Optional

from daffi.utils import colors
from daffi.utils.logger import get_daffi_logger
from daffi.exceptions import InitializationError

logger = get_daffi_logger("registry", colors.blue)

_base_class_defined = False


class RegistryMeta(ABCMeta):
    def __new__(mcs, cls_name: str, bases: Tuple[Type[Any], ...], namespace: Dict[str, Any], **kwargs: Any) -> Type:

        if _base_class_defined:
            daffi_mro = []
            is_registry_class = cls_name in ("BaseRegistry", "Callback", "Fetcher")
            auto_init = None
            if not is_registry_class:
                from daffi.registry import Callback
                from daffi.registry import Fetcher

                _registry_methods = (Fetcher, Callback)

                for _cls in bases:
                    if hasattr(_cls, "__daffi_mro__"):
                        # Take `__daffi_mro__` from base class if exists.
                        daffi_mro.extend(_cls.__daffi_mro__)

                    if _cls in _registry_methods and _cls not in daffi_mro:
                        if not daffi_mro:
                            daffi_mro.append(_cls)
                        else:
                            InitializationError(
                                f"Inheritance for a class {cls_name!r} can be derived from either"
                                f" {Fetcher} or {Callback}. Multiple inheritance is prohibited."
                            ).fire()

                if (auto_init := namespace.get("auto_init", None)) is None:
                    for base in bases:
                        if (auto_init := getattr(base, "auto_init", None)) is not None:
                            namespace.update(auto_init=auto_init)
                            break
            # Store all parent daffi classes to `__daffi_mro__` class attribute
            namespace.update(__daffi_mro__=daffi_mro)
            cls = _type = super().__new__(mcs, cls_name, bases, namespace, **kwargs)  # type: ignore

            if not is_registry_class:
                if auto_init:
                    _type = _type()

                for _initializator_cls in daffi_mro:
                    _initializator_cls._init_class(cls, _type)
            return cls
        else:
            # this is the RegistryMeta class itself being created.
            return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


_base_class_defined = True


class BaseRegistry(metaclass=RegistryMeta):
    __slots__ = "__dict__"
    __doc__ = ""  # Null out the Representation docstring

    def __str__(self):
        return f"{self.__class__.__name__}<{', '.join([cl.__name__ for cl in getattr(self, '__daffi_mro__', [])])}>"

    __repr__ = __str__

    _ipc: ClassVar["Ipc"] = None
    _g: ClassVar["Global"] = None

    def __init__(self):
        for base in self.__daffi_mro__:
            base._post_init(self)
        self.__post_init__()

    def __post_init__(self):
        """For additional user specific initialization"""
        pass

    @property
    def g(self):
        return self._g

    @staticmethod
    @abstractmethod
    def _init_class(cls, instance_or_type):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def _init_function(cls, fn: Callable[..., Any], fn_name: Optional[str] = None):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def _post_init(self):
        raise NotImplementedError()

    @staticmethod
    def _get_alias(self, wrapped) -> Optional[str]:
        """Get custom executor alias"""
        if hasattr(self, "alias"):
            return self.alias
        elif hasattr(wrapped, "alias"):
            return wrapped.alias
