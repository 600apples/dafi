import os
import inspect
from typing import Callable, Any
from dafi.utils.custom_types import P
from dafi.utils.mappings import NODE_CALLBACK_MAPPING, WELL_KNOWN_CALLBACKS


def get_class_methods(klass):
    return [
        member
        for member in [getattr(klass, attr) for attr in dir(klass)]
        if inspect.isfunction(member) or inspect.ismethod(member)
    ]


def is_class_or_static_method(klass: type, name: str):
    """Test if a value of a class is static method or class method.

    example::

        class MyClass(object):
            @staticmethod
            def method():
                ...

    Args:
        :klass: class type
        :name: attribute name
    """

    value = getattr(klass, name)
    assert getattr(klass, name) == value

    for cls in inspect.getmro(klass):
        if inspect.isroutine(value):
            if name in cls.__dict__:
                bound_value = cls.__dict__[name]
                if isinstance(bound_value, (staticmethod, classmethod)):
                    return True
    return False


def pretty_callbacks(exclude_proc, format: str):
    res = "" if format == "string" else dict()
    for proc, func_mapping in NODE_CALLBACK_MAPPING.items():
        if proc != exclude_proc:
            available_functions = [fn for fn in func_mapping if fn not in WELL_KNOWN_CALLBACKS]
            if format == "string":
                res += f"process: {proc}\n"
                res += f"  - [ {', '.join(available_functions) or '<< no registered callbacks >>'} ]\n"
            else:
                res[proc] = available_functions
        return res


def func_info(func: Callable[P, Any]):
    """
    Return the function import path (as a list of module names), and
    a name for the function.
    """
    if hasattr(func, "__module__"):
        module = func.__module__
    else:
        try:
            module = inspect.getmodule(func)
        except TypeError:
            if hasattr(func, "__class__"):
                module = func.__class__.__module__
            else:
                module = "unknown"
    if module is None:
        module = ""
    if module == "__main__":
        try:
            filename = os.path.abspath(inspect.getsourcefile(func))
        except Exception:
            filename = None
        if filename is not None:
            if filename.endswith(".py"):
                filename = filename[:-3]
            module = module + "-" + filename
    module = module.split(".")
    if hasattr(func, "func_name"):
        name = func.func_name
    elif hasattr(func, "__name__"):
        name = func.__name__
    else:
        name = "unknown"
    # Hack to detect functions not defined at the module-level
    if hasattr(func, "func_globals") and name in func.func_globals:
        if not func.func_globals[name] is func:
            name = "%s-alias" % name
    if inspect.ismethod(func):
        # We need to add the name of the class
        if hasattr(func, "im_class"):
            klass = func.im_class
            module.append(klass.__name__)
    return module, name
