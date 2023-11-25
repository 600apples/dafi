from inspect import iscoroutinefunction, isgeneratorfunction, isclass, ismethod, isfunction, getmembers
from daffi.exceptions import InitializationError
from daffi.utils.misc import is_lambda_function
from daffi.registry.executor import EXECUTOR_REGISTRY


__all__ = ["callback"]


class callback:
    """
    Example:
        >>> from daffi import callback
        >>>
        >>> @callback
        >>> def my_func(*args, **kwargs):
                ...
    """

    def __init__(self, func_or_class):

        cls = None
        self._func_or_class = func_or_class
        if isclass(func_or_class):
            cls = func_or_class()
            members = getmembers(func_or_class, predicate=lambda x: isfunction(x) or ismethod(x))
        else:
            members = [(func_or_class.__name__, func_or_class)]
        for name, func in members:

            if name.startswith("_") or hasattr(func, "local"):
                # Ignore methods which starts with `_` and methods decorated with `local` decorator.
                continue

            if is_lambda_function(func):
                raise InitializationError(f"Not supported. {name!r} is lambda function.")

            if iscoroutinefunction(func):
                raise InitializationError(f"Not supported. {name!r} is coroutine function.")

            if isgeneratorfunction(func):
                raise InitializationError(f"Not supported. {name!r} is generator function.")

            EXECUTOR_REGISTRY.register(name=name, func=func, cls=cls)

    def __call__(self, *args, **kwargs):
        return self._func_or_class(*args, **kwargs)
#
#
# class CallbackGroup(BaseRegistry):
#     """
#     Remote callback group representation. All public methods of class inherited from Callbacks become remote callbacks
#     identified by their names.
#     """
#
#
#     @staticmethod
#     def _init_class(cls, instance):
#         """Register all public methods of class as callbacks."""
#         # Iterate over all methods of class.
#         for method in get_class_methods(cls):
#             if method.__name__.startswith("_") or hasattr(method, "local"):
#                 # Ignore methods which starts with `_` and methods decorated with `local` decorator.
#                 continue
#
#             if iscoroutinefunction(method):
#                 raise InitializationError(f"Not supported. {method} is coroutine function.")
#
#             if isgeneratorfunction(method):
#                 raise InitializationError(f"Not supported. {method} is generator function.")
#
#             _, method_alias = func_info(method)
#             if not hasattr(cls, method_alias):
#                 # If method has alias then add this method to class initialization by alias
#                 setattr(cls, method_alias, method)
#             cb = ClassCallbackExecutor(
#                 cls=instance,
#                 # Origin name is the name callback is visible for other remote processes
#                 origin_name_=method_alias,
#             )
#             if existing := LOCAL_CALLBACK_MAPPING.get(method_alias):
#                 raise InitializationError(
#                     f"Callback {existing!r} is already registered. "
#                     f"Please, use another name or set custom alias to callback."
#                 )
#             LOCAL_CALLBACK_MAPPING[method_alias] = cb
#             logger.info(f"callback {method_alias!r} <class {cls.__name__}> is registered")
#         return instance
#
#     @classmethod
#     def _init_function(cls, fn: Callable[..., Any], fn_name: Optional[str] = None) -> CallbackExecutor:
#         """Register one function as remote callback (This method is used in `callback` decorator)."""
#
#         if is_lambda_function(fn):
#             raise InitializationError(f"Not supported. {fn} is lambda function.")
#
#         if iscoroutinefunction(fn):
#             raise InitializationError(f"Not supported. {fn} is coroutine function.")
#
#         if isgeneratorfunction(fn):
#             raise InitializationError(f"Not supported. {fn} is generator function.")
#
#         elif isinstance(fn, type):
#             # Class wrapped
#             raise InitializationError(
#                 "Classes are not supported."
#                 " Use `CallbackGroup` base class from `daffi.registry` "
#                 "package to initialize class as callback group"
#             )
#
#         if fn_name:
#             name = fn_name
#         else:
#             _, name = func_info(fn)
#         _fn = CallbackExecutor(
#             wrapped=fn,
#             origin_name_=name,
#         )
#         if existing := LOCAL_CALLBACK_MAPPING.get(name):
#             raise InitializationError(
#                 f"Callback {existing!r} is already registered. "
#                 f"Please, use another name or set custom alias to callback."
#             )
#         LOCAL_CALLBACK_MAPPING[name] = _fn
#         logger.info(f"callback {name!r} is registered")
#         return _fn
#
#     @staticmethod
#     def _get_alias(self, wrapped) -> Optional[str]:
#         """Get custom executor alias"""
#         if hasattr(self, "alias"):
#             return self.alias
#         elif hasattr(wrapped, "alias"):
#             return wrapped.alias
