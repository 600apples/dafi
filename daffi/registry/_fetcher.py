from typing import Callable, Any, Union, Type, Optional, ClassVar, Tuple, Dict
from inspect import iscoroutinefunction, isasyncgenfunction, isgeneratorfunction
from daffi.registry._base import BaseRegistry, logger
from daffi.method_executors import FetcherExecutor, ClassFetcherExecutor
from daffi.utils.func_validation import (
    get_class_methods,
    func_info,
    is_class_or_static_method,
)
from daffi.utils.custom_types import P
from daffi.exceptions import InitializationError
from daffi.utils.misc import is_lambda_function, contains_explicit_return
from daffi.settings import LOCAL_FETCHER_MAPPING
from daffi.execution_modifiers import FG, BG, BROADCAST, PERIOD

__all__ = ["Fetcher", "FetcherMethod", "Args"]


PROTECTED_METHODS = ["make_fetcher_method"]


class Fetcher(BaseRegistry):
    # Default execution modifier for all Fetcher's methods
    exec_modifier: ClassVar[Union[FG, BG, BROADCAST, PERIOD]] = FG

    def __getattribute__(self, item):
        return LOCAL_FETCHER_MAPPING.get(f"{id(self)}-{item}", super().__getattribute__(item))

    @staticmethod
    def make_fetcher_method(exec_modifier: Union[FG, BG, BROADCAST, PERIOD] = None):
        """
        Create static fetcher method from `FetcherMethod` property
        Example:
            >>> class T(Fetcher):
            >>>
            >>>     def __post_init__(self):
            >>>         self.my_fetcher = self.make_fetcher_method()
        """
        return FetcherMethod(exec_modifier=exec_modifier)

    def __setattr__(self, key, value):
        if isinstance(value, FetcherMethod):

            def fetcher_method(*args, **kwargs):
                ...

            setattr(self.__class__, key, staticmethod(fetcher_method))
            getattr(self.__class__, key).__name__ = key

            cb = ClassFetcherExecutor(
                klass=self,
                origin_name_=key,
                origin_method=key,
                is_async=False,
                is_static=True,
                proxy_=True,
                exec_modifier_=value.exec_modifier or self.__class__.exec_modifier,
                is_generator=False,
            )
            LOCAL_FETCHER_MAPPING[f"{id(self)}-{key}"] = cb
            logger.info(f"fetcher {key!r} <class {self.__class__.__name__}> is registered. (proxy=False)")

        else:
            super().__setattr__(key, value)

    @staticmethod
    def _init_class(cls, instance_or_type) -> Union[Type, "Fetcher"]:
        """
        Register all public methods of class as fetchers.
        Args:
            instance_or_type: class type or class instance. This argument depends on `auto_init` value of base class
                if `auto_init` == True then class instance will be instantiated implicitly.
        """

        exec_modifiers_mapping = dict()

        for attr_name in dir(cls):
            attr = getattr(cls, attr_name)
            if isinstance(attr, FetcherMethod):

                def fetcher_method(*args, **kwargs):
                    ...

                setattr(cls, attr_name, staticmethod(fetcher_method))
                getattr(cls, attr_name).__name__ = attr_name
                # Custom exec modifier will be using with fetcher method. if exec_modifier is None then
                # default exec modifier will be assigned from class
                exec_modifiers_mapping[attr_name] = attr.exec_modifier

        # Iterate over all methods of class.
        for method in get_class_methods(cls):
            _, method_alias = func_info(method)
            origin_method_name = method.__name__

            if (
                origin_method_name.startswith("_")
                or hasattr(method, "local")
                or origin_method_name in PROTECTED_METHODS
            ):
                # Ignore methods which starts with `_` and methods decorated with `local` decorator.
                continue

            # Check if method is static or classmethod.
            # Remote fetcher is ready to use in 2 cases:
            #    1. Instance of class is not instantiated (`auto_init` == False)
            #           but method is staticmethod or classmethod. static/class methods
            #           are not bounded to specific instance
            #    2. Instance of class is instantiated (`auto_init` == True). In this case all public methods
            #           become remote callbacks.
            if isasyncgenfunction(method):
                InitializationError(f"Method {method} has wrong type. Async generators are not supported yet.").fire()

            if is_generator := isgeneratorfunction(method):
                # Disable proxy for method implicitly. To initialize stream fetcher
                # should have one or more yield statements and fetcher's body should be used to process this stream
                is_proxy = False
            else:
                # Infer if method is proxy based on method's body. If method contains `return` statement then
                # proxy must be False
                is_proxy = not contains_explicit_return(method)

            is_static_or_class_method = is_class_or_static_method(cls, origin_method_name)
            if not isinstance(instance_or_type, type):
                klass = instance_or_type
            else:
                klass = cls if is_static_or_class_method else None
            cb = ClassFetcherExecutor(
                klass=klass,
                # Origin name is pointer to remote callback. If `alias` decorator is used then this value is
                # different from `origin_method_name`
                origin_name_=method_alias,
                origin_method=getattr(klass, origin_method_name) if klass else None,
                is_async=iscoroutinefunction(method),
                is_static=str(is_static_or_class_method) == "static",
                # Proxy flag indicates whether method body is used as source for remote args/kwargs.
                # If proxy=True then method works as proxy (without body execution).
                # IOW when proxy=True all provided arguments will be passed to remote callback as is.
                proxy_=is_proxy,
                exec_modifier_=exec_modifiers_mapping.get(origin_method_name) or cls.exec_modifier,
                is_generator=is_generator,
            )
            LOCAL_FETCHER_MAPPING[f"{cls.__name__}-{origin_method_name}"] = cb
            logger.info(f"fetcher {origin_method_name!r} <class {cls.__name__}> is registered. (proxy={is_proxy})")

        return instance_or_type

    @classmethod
    def _init_function(
        cls,
        fn: Callable[..., Any],
        fn_name: Optional[str] = None,
        exec_modifier: Union[FG, BG, BROADCAST, PERIOD] = FG,
    ) -> FetcherExecutor:
        """Register one function as remote fetcher (This method is used in `fetcher` decorator)."""
        from daffi.decorators import callback

        is_async = is_generator = is_proxy = False

        if hasattr(fn, "__func__"):
            # Bypass staticmethod/classmethod decorators
            fn = fn.__func__

        if isinstance(fn, callback):
            is_async = fn._fn.is_async
            fn = fn._fn.wrapped
            is_generator = False
            # Enable proxy forcibly if function is used as fetcher and callback at once.
            # In this case function's body is used only by callback part
            is_proxy = True

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
            is_generator = isgeneratorfunction(fn)
            # Disable proxy for function implicitly. To initialize stream fetcher
            # should have one or more yield statements and fetcher's body should be used to process this stream
            is_proxy = False if is_generator else not contains_explicit_return(fn)

        else:
            InitializationError(f"Type {type(fn)} is not supported.")

        if isasyncgenfunction(fn):
            InitializationError(f"Function {fn} has wrong type. Async generators are not supported yet.").fire()

        if fn_name:
            name = fn_name
        else:
            _, name = func_info(fn)

        _fn = FetcherExecutor(
            wrapped=fn,
            origin_name_=name,
            is_async=is_async,
            # Proxy flag indicates whether method body is used as source for remote args/kwargs.
            # If proxy=True then method works as proxy (without body execution).
            # IOW when proxy=True all provided arguments will be passed to remote callback as is.
            proxy_=is_proxy,
            exec_modifier_=exec_modifier,
            is_generator=is_generator,
        )
        LOCAL_FETCHER_MAPPING[f"{id(fn)}-{name}"] = _fn
        logger.info(f"fetcher {name!r} is registered. (proxy={is_proxy})")
        return _fn

    @staticmethod
    def _post_init(self):
        """Initialize instance of Fetcher class on demand (If this class is not initialized implicitly yet)."""

        for method in get_class_methods(self.__class__):
            origin_method_name = method.__name__

            if origin_method_name.startswith("_") or hasattr(method, "local"):
                continue

            if info := LOCAL_FETCHER_MAPPING.get(f"{self.__class__.__name__}-{origin_method_name}"):
                # Get origin method from super. Default getattr might return fetcher instance if method has alias.
                origin_method = super().__getattribute__(origin_method_name)
                LOCAL_FETCHER_MAPPING[f"{id(self)}-{origin_method_name}"] = info._replace(
                    klass=self, origin_method=origin_method
                )


class FetcherMethod:
    def __init__(self, exec_modifier: Union[FG, BG, BROADCAST, PERIOD] = None):
        self.exec_modifier = exec_modifier

    def __call__(self, *args, **kwargs):
        raise InitializationError(
            "Ensure that `FetcherMethod` is a property at the class level,"
            " and not instantiated within __init__, __post_init__, or any other instance methods."
        )


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
