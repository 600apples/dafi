from inspect import signature, iscoroutinefunction
from typing import Callable, Any, Union, Type, Optional
from daffi.registry._base import BaseRegistry, logger
from daffi.callback_types import RemoteClassCallback, RemoteCallback
from daffi.utils.func_validation import (
    get_class_methods,
    func_info,
    is_class_or_static_method,
)
from daffi.exceptions import InitializationError
from daffi.utils.misc import is_lambda_function
from daffi.utils.settings import LOCAL_CALLBACK_MAPPING, WELL_KNOWN_CALLBACKS, LOCAL_CLASS_CALLBACKS


__all__ = ["Callback"]


class Callback(BaseRegistry):
    """
    Remote callback group representation. All public methods of class inherited from Callbacks become remote callbacks
    identified by they names.
    """

    @classmethod
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
            _, name = func_info(method)

            if name.startswith("_"):
                # Ignore methods which starts with `_`.
                continue

            # Check if method is static or classmethod.
            # Remote callback is ready to use in 2 cases:
            #    1. Instance of class is not instantiated (`auto_init` == False)
            #           but method is staticmethod or classmethod. static/class methods
            #           are not bounded to specific instance
            #    2. Instance of class is instantiated (`auto_init` == True). In this case all public methods
            #           become remote callbacks.
            is_static_or_class_method = is_class_or_static_method(cls, name)
            if not isinstance(instance_or_type, type):
                klass = instance_or_type
            else:
                klass = cls if is_static_or_class_method else None
            cb = RemoteClassCallback(
                klass=klass,
                klass_name=cls.__name__,
                origin_name=name,
                signature=signature(method),
                is_async=iscoroutinefunction(method),
                is_static=str(is_static_or_class_method) == "static",
            )
            name_in_mapping = name in LOCAL_CALLBACK_MAPPING
            LOCAL_CALLBACK_MAPPING[name] = cb
            if not name_in_mapping:
                logger.info(f"{name!r} registered" + ("" if klass else f" (required {cls.__name__} initialization)"))

            _updated = True

        if _updated:
            cls._update_callbacks()

        return instance_or_type

    @classmethod
    def _init_function(cls, fn: Callable[..., Any], fn_name: Optional[str] = None) -> RemoteCallback:
        """Register one function as remote callback (This method is used in `callback` decorator)."""
        if is_lambda_function(fn):
            InitializationError("Lambdas is not supported.").fire()

        if fn_name:
            name = fn_name
        else:
            _, name = func_info(fn)
        _fn = RemoteCallback(
            callback=fn,
            origin_name=name,
            signature=signature(fn),
            is_async=iscoroutinefunction(fn),
        )
        LOCAL_CALLBACK_MAPPING[name] = _fn
        if name not in WELL_KNOWN_CALLBACKS:
            logger.info(f"{name!r} registered")

        cls._update_callbacks()
        return _fn

    @classmethod
    def _build_class_callback_instance(cls, klass, *args, **kwargs):
        """
        Initialize instance of Callback class on demand (If this class is not initialized implicitly yet).
        Args:
            klass: class type
            args: positional arguments that class takes in __init__ method
            kwargs: keyword arguments that class takes in __init__ method
        """
        if isinstance(klass, type):
            class_name = klass.__name__
        else:
            class_name = klass.__class__.__name__
        if class_name in LOCAL_CLASS_CALLBACKS:
            InitializationError(f"Only one callback instance of {class_name!r} should be created.").fire()

        LOCAL_CLASS_CALLBACKS.add(klass.__name__)
        klass = klass(*args, **kwargs)

        method_initialized = False
        for method in get_class_methods(klass):
            module, name = func_info(method)
            if name.startswith("_"):
                continue

            info = LOCAL_CALLBACK_MAPPING.get(name)
            if info:
                method_initialized = True
                LOCAL_CALLBACK_MAPPING[name] = info._replace(klass=klass)

        if method_initialized:
            cls._update_callbacks()
        return klass
