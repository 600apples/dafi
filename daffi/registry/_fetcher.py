from inspect import iscoroutinefunction, isasyncgenfunction
from typing import Callable, Any, Union, Type, Optional, ClassVar, Tuple, Dict
from daffi.registry._base import BaseRegistry
from daffi.method_executors import FetcherExecutor, ClassFetcherExecutor
from daffi.utils.func_validation import (
    get_class_methods,
    func_info,
    is_class_or_static_method,
)
from daffi.utils.custom_types import P
from daffi.exceptions import InitializationError
from daffi.utils.misc import is_lambda_function
from daffi.settings import LOCAL_FETCHER_MAPPING

from daffi.execution_modifiers import FG, BG, BROADCAST, STREAM, PERIOD


__all__ = ["Fetcher", "Args"]


class Fetcher(BaseRegistry):

    # Default execution modifier for all Fetcher's methods
    exec_modifier: ClassVar[Union[FG, BG, BROADCAST, STREAM, PERIOD]] = FG
    # Proxy flag indicates whether method body is used as source for remote args/kwargs.
    # If proxy=True then method works as proxy (without body execution).
    # IOW when proxy=True all provided arguments will be passed to remote callback as is.
    proxy: ClassVar[bool] = True

    def __getattribute__(self, item):
        return LOCAL_FETCHER_MAPPING.get(f"{id(self)}-{item}", super().__getattribute__(item))

    @classmethod
    def _init_class(cls, instance_or_type) -> Union[Type, "Callback"]:
        """
        Register all public methods of class as fetchers.
        Args:
            instance_or_type: class type or class instance. This argument depends on `auto_init` value of base class
                if `auto_init` == True then class instance will be instantiated implicitly.
        """
        # Iterate over all methods of class.
        for method in get_class_methods(cls):
            _, name = func_info(method)

            if name.startswith("_"):
                # Ignore methods which starts with `_`.
                continue

            # Check if method is static or classmethod.
            # Remote fetcher is ready to use in 2 cases:
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
            cb = ClassFetcherExecutor(
                klass=klass,
                origin_name_=name,
                origin_method=getattr(klass, name) if klass else None,
                is_async=iscoroutinefunction(method),
                is_static=str(is_static_or_class_method) == "static",
                proxy_=cls.proxy,
                exec_modifier_=cls.exec_modifier,
            )
            LOCAL_FETCHER_MAPPING[f"{cls.__name__}-{name}"] = cb
        return instance_or_type

    @classmethod
    def _init_function(
        cls,
        fn: Callable[..., Any],
        fn_name: Optional[str] = None,
        proxy: Optional[bool] = True,
        exec_modifier: Union[FG, BG, BROADCAST, STREAM, PERIOD] = FG,
    ) -> FetcherExecutor:
        """Register one function as remote fetcher (This method is used in `fetcher` decorator)."""
        from daffi.decorators import callback

        is_async = False

        if isasyncgenfunction(fn):
            InitializationError(f"Async generators are not supported yet.").fire()

        elif isinstance(fn, callback):
            proxy = True
            is_async = fn._fn.is_async
            fn = fn._fn.wrapped

        elif isinstance(fn, type):
            # Class wrapped
            InitializationError(
                "Classes are not supported."
                " Use `Fetcher` base class from `daffi.registry` "
                "package to initialize class as fetcher group"
            ).fire()

        elif callable(fn):
            if is_lambda_function(fn):
                InitializationError("Lambdas are not supported.").fire()
            is_async = iscoroutinefunction(fn)

        else:
            InitializationError(f"Type {type(fn)} is not supported.")

        if fn_name:
            name = fn_name
        else:
            _, name = func_info(fn)

        _fn = FetcherExecutor(
            wrapped=fn, origin_name_=name, is_async=is_async, proxy_=proxy, exec_modifier_=exec_modifier
        )
        LOCAL_FETCHER_MAPPING[f"{id(fn)}-{name}"] = _fn
        return _fn

    def _post_init(self):
        """Initialize instance of Fetcher class on demand (If this class is not initialized implicitly yet)."""

        for method in get_class_methods(self.__class__):
            _, name = func_info(method)
            if name.startswith("_"):
                continue

            if info := LOCAL_FETCHER_MAPPING.get(f"{self.__class__.__name__}-{name}"):
                origin_method = getattr(self, name)
                LOCAL_FETCHER_MAPPING[f"{id(self)}-{name}"] = info._replace(klass=self, origin_method=origin_method)


class Args:
    """Arguments aggregator for fetchers without proxy enabled"""

    def __init__(self, *args: P.args, **kwargs: P.kwargs):
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def _aggregate_args(cls, args: Any) -> Tuple[Tuple, Dict]:
        """
        Split provided arguments to positional and keyword arguments.
        There are 2 possible options:
            1. If provided args is type of `Args` then use args and kwargs from it
            2. If provides args is tuple then use tuple as args and set kwargs as empty dict.
        """
        kwargs = {}
        if isinstance(args, cls):
            args, kwargs = args.args, args.kwargs
        elif not isinstance(args, tuple):
            args = (args,)
        return args, kwargs
