from inspect import signature, iscoroutinefunction, isgeneratorfunction
from typing import Callable, Any, Union, Type, Optional, ClassVar
from daffi.registry._base import BaseRegistry, logger
from daffi.method_executors import ClassCallbackExecutor, CallbackExecutor
from daffi.utils.func_validation import (
    get_class_methods,
    func_info,
    is_class_or_static_method,
)
from daffi.exceptions import InitializationError
from daffi.utils.misc import is_lambda_function
from daffi.settings import LOCAL_CALLBACK_MAPPING, WELL_KNOWN_CALLBACKS, LOCAL_CLASS_CALLBACKS


__all__ = ["Callback"]


class Callback(BaseRegistry):
    """
    Remote callback group representation. All public methods of class inherited from Callbacks become remote callbacks
    identified by they names.
    """

    # If auto_init=True then class will be implicitly instantiated.
    auto_init: ClassVar[bool] = True

    @staticmethod
    def _init_class(cls, instance_or_type) -> Union[Type, "Callback"]:
        """
        Register all public methods of class as callbacks.
        Args:
            instance_or_type: class type or class instance. This argument depends on `auto_init` value of base class
                if `auto_init` == True then class instance will be instantiated implicitly.
        """
        _updated = False
        # Iterate over all methods of class.
        for method in get_class_methods(cls):
            _, method_alias = func_info(method)
            origin_method_name = method.__name__

            if origin_method_name.startswith("_") or hasattr(method, "local"):
                # Ignore methods which starts with `_` and methods decorated with `local` decorator.
                continue

            if not hasattr(cls, method_alias):
                # If method has alias then add this method to class initialization by alias
                setattr(cls, method_alias, method)

            # Check if method is static or classmethod.
            # Remote callback is ready to use in 2 cases:
            #    1. Instance of class is not instantiated (`auto_init` == False)
            #           but method is staticmethod or classmethod. static/class methods
            #           are not bounded to specific instance
            #    2. Instance of class is instantiated (`auto_init` == True). In this case all public methods
            #           become remote callbacks.
            is_static_or_class_method = is_class_or_static_method(cls, origin_method_name)
            if not isinstance(instance_or_type, type):
                klass = instance_or_type
            else:
                klass = cls if is_static_or_class_method else None
            cb = ClassCallbackExecutor(
                klass=klass,
                # Origin name is the name callback is visible for other remote processes
                origin_name_=method_alias,
                signature=signature(method),
                is_async=iscoroutinefunction(method),
                is_static=str(is_static_or_class_method) == "static",
                is_generator=isgeneratorfunction(method),
            )
            name_in_mapping = method_alias in LOCAL_CALLBACK_MAPPING
            LOCAL_CALLBACK_MAPPING[method_alias] = cb
            if not name_in_mapping:
                logger.info(
                    f"callback {method_alias!r} is registered"
                    + ("" if klass else f" (required {cls.__name__} initialization)")
                )

            _updated = True

        if _updated:
            cls._update_callbacks()

        return instance_or_type

    @classmethod
    def _init_function(cls, fn: Callable[..., Any], fn_name: Optional[str] = None) -> CallbackExecutor:
        """Register one function as remote callback (This method is used in `callback` decorator)."""
        if is_lambda_function(fn):
            InitializationError("Lambdas is not supported.").fire()

        elif isinstance(fn, type):
            # Class wrapped
            InitializationError(
                "Classes are not supported."
                " Use `Callback` base class from `daffi.registry` "
                "package to initialize class as callback group"
            ).fire()

        if fn_name:
            name = fn_name
        else:
            _, name = func_info(fn)
        _fn = CallbackExecutor(
            wrapped=fn,
            origin_name_=name,
            signature=signature(fn),
            is_async=iscoroutinefunction(fn),
            is_generator=isgeneratorfunction(fn),
        )
        LOCAL_CALLBACK_MAPPING[name] = _fn
        if name not in WELL_KNOWN_CALLBACKS:
            logger.info(f"callback {name!r} is registered")

        cls._update_callbacks()
        return _fn

    @classmethod
    def _update_callbacks(cls):
        if cls._ipc and cls._ipc.is_running:
            # Update remote callbacks if ips is running. It means callback was not registered during handshake
            # or callback was added dynamically.
            cls._ipc.update_callbacks()

    @staticmethod
    def _post_init(self):
        """Initialize instance of Callback class on demand (If this class is not initialized implicitly yet)."""
        class_name = self.__class__.__name__
        if class_name in LOCAL_CLASS_CALLBACKS:
            msg = f"Only one callback instance of {class_name!r} should be created."
            if self.auto_init:
                msg += (
                    " auto_init is enabled for this class. "
                    "If you want to create instance explicitly "
                    "then you need specify `auto_init=False` class attribute for this class"
                )
            InitializationError(msg).fire()
        LOCAL_CLASS_CALLBACKS.add(class_name)

        if not self.auto_init:
            method_initialized = False
            for method in get_class_methods(self.__class__):
                module, name = func_info(method)
                if name.startswith("_"):
                    continue

                if info := LOCAL_CALLBACK_MAPPING.get(name):
                    method_initialized = True
                    LOCAL_CALLBACK_MAPPING[name] = info._replace(klass=self)

            if method_initialized:
                self._update_callbacks()
