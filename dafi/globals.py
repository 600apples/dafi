import os
import inspect
import asyncio
from itertools import islice
from functools import cached_property, wraps
from typing import Callable, Any, Coroutine, Optional, NamedTuple, Generic, List, Tuple, Dict
from typing_extensions import ParamSpec
from dafi.backend import LocalBackEnd
from dafi.ipc import Ipc
from dafi.utils import (
    Singleton,
    K,
    GlobalCallback,
    CALLBACK_MAPPING,
    CALLBACKS_WITH_CUSTOM_NAMES,
)

P = ParamSpec("P")


class CallbackInfo(NamedTuple):
    callback: GlobalCallback
    module: str
    origin_name: str
    signature: Tuple[Tuple, Dict]
    code: str


class Global(metaclass=Singleton):

    def __init__(
            self,
            process_name: Optional[str] = None,
            init_master: Optional[bool] = True,
            init_node: Optional[bool] = True,
    ):
        """
        Args:
            process_name: Global process name. If specified it is used as reference key for callback response.
                By default process pid is used as reference.
        """
        self.process_name = str(process_name or os.getpid())

        # No reason to init node if no callbacks specified.
        self.ipc = Ipc(
            process_name=self.process_name,
            backend=LocalBackEnd(),
            init_master=init_master,
            init_node=init_node
        )
        self.ipc.start()

    def __getattr__(self, item):
        return self.ipc.call(func_name=item)

    @property
    def is_master(self) -> bool:
        return bool(self.ipc.master)

    @property
    def registered_callbacks(self) -> List[str]:
        return list(CALLBACK_MAPPING)


class callback(Generic[GlobalCallback]):
    def __init__(
        self,
        fn: Optional[Callable[P, Any]] = None,
        remote_name: Optional[str] = None,
        async_: Optional[bool] = True,
    ):
        if callable(fn):
            # Only fn was provided eg using callback without execution
            self.fn = fn
            self.remote_name = remote_name
            self.async_ = async_

        elif isinstance(fn, str):
            # Use callback with execution eg @callback(
            self.remote_name = fn
        elif isinstance(fn, bool):
            self.async_ = fn
        if isinstance(remote_name, bool):
            self.async_ = remote_name

        if self.fn:
            module, name = self.func_info
            self.remote_name = self.remote_name or name

            info = CallbackInfo(
                callback=self,
                module=module,
                origin_name=name,
                code=self.func_code,
                signature=self.func_signature
            )
            CALLBACK_MAPPING[self.remote_name] = info
            if self.remote_name != name:
                CALLBACKS_WITH_CUSTOM_NAMES.add(name)


    def __call__(self, *args: P.args, **kwargs: P.kwargs):
        @wraps(self.fn)
        async def __async_call__(result: Coroutine):
            return await result

        result = self.fn(*args, **kwargs)
        if asyncio.iscoroutine(result):
            result = __async_call__(result)
        return result


    @cached_property
    def func_info(self):
        """
        Return the function import path (as a list of module names), and
        a name for the function.
        """
        if hasattr(self.fn, "__module__"):
            module = self.fn.__module__
        else:
            try:
                module = inspect.getmodule(self.fn)
            except TypeError:
                if hasattr(self.fn, "__class__"):
                    module = self.fn.__class__.__module__
                else:
                    module = "unknown"
        if module is None:
            module = ""
        if module == "__main__":
            try:
                filename = os.path.abspath(inspect.getsourcefile(self.fn))
            except Exception:
                filename = None
            if filename is not None:
                if filename.endswith(".py"):
                    filename = filename[:-3]
                module = module + "-" + filename
        module = module.split(".")
        if hasattr(self.fn, "func_name"):
            name = self.fn.func_name
        elif hasattr(self.fn, "__name__"):
            name = self.fn.__name__
        else:
            name = "unknown"
        # Hack to detect functions not defined at the module-level
        if hasattr(self.fn, "func_globals") and name in self.fn.func_globals:
            if not self.fn.func_globals[name] is self.fn:
                name = "%s-alias" % name
        if inspect.ismethod(self.fn):
            # We need to add the name of the class
            if hasattr(self.fn, "im_class"):
                klass = self.fn.im_class
                module.append(klass.__name__)
        return module, name

    @cached_property
    def func_code(self):
        """ Attempts to retrieve a reliable function code hash.

            The reason we don't use inspect.getsource is that it caches the
            source, whereas we want this to be modified on the fly when the
            function is modified.

            Returns
            -------
            func_code: string
                The function code
            source_file: string
                The path to the file in which the function is defined.
            first_line: int
                The first line of the code in the source file.

            Notes
            ------
            This function does a bit more magic than inspect, and is thus
            more robust.
        """
        source_file = None
        try:
            code = self.fn.__code__
            source_file = code.co_filename
            if not os.path.exists(source_file):
                # Use inspect for lambda functions and functions defined in an
                # interactive shell, or in doctests
                source_code = ''.join(inspect.getsourcelines(self.fn)[0])
                line_no = 1
                if source_file.startswith('<doctest '):
                    source_file, line_no = re.match(
                        r'\<doctest (.*\.rst)\[(.*)\]\>', source_file).groups()
                    line_no = int(line_no)
                    source_file = '<doctest %s>' % source_file
                return source_code, source_file, line_no
            # Try to retrieve the source code.
            with open(source_file) as source_file_obj:
                first_line = code.co_firstlineno
                # All the lines after the function definition:
                source_lines = list(islice(source_file_obj, first_line - 1, None))
            return ''.join(inspect.getblock(source_lines)), source_file, first_line
        except Exception:
            # If the source code fails, we use the hash. This is fragile and
            # might change from one session to another.
            if hasattr(self.fn, '__code__'):
                return str(self.fn.__code__.__hash__()), source_file, -1

    @cached_property
    def func_signature(self):
        """Get Dict of filtered positional and keyword arguments"""
        args = list()
        kwargs = dict()
        # Special case for functools.partial objects
        if not inspect.ismethod(self.fn) and not inspect.isfunction(self.fn):
            raise ValueError(f"Provided callable {self.fn} is not function nor method")
        arg_sig = inspect.signature(self.fn)
        arg_names = []
        arg_defaults = []
        arg_kwonlyargs = []
        arg_varargs = None
        arg_varkw = None
        for param in arg_sig.parameters.values():
            if param.kind is param.POSITIONAL_OR_KEYWORD:
                arg_names.append(param.name)
            elif param.kind is param.KEYWORD_ONLY:
                arg_names.append(param.name)
                arg_kwonlyargs.append(param.name)
            elif param.kind is param.VAR_POSITIONAL:
                arg_varargs = param.name
            elif param.kind is param.VAR_KEYWORD:
                arg_varkw = param.name
            if param.default is not param.empty:
                arg_defaults.append(param.default)
        if inspect.ismethod(self.fn):
            # First argument is 'self', it has been removed by Python
            # we need to add it back:
            args = [
                self.fn.__self__,
            ] + args
            # func is an instance method, inspect.signature(func) does not
            # include self, we need to fetch it from the class method, i.e
            # func.__func__
            class_method_sig = inspect.signature(self.fn.__func__)
            self_name = next(iter(class_method_sig.parameters))
            arg_names = [self_name] + arg_names

        _, name = self.func_info
        arg_dict = dict()
        arg_position = -1
        for arg_position, arg_name in enumerate(arg_names):
            if arg_position < len(args):
                pass
            #         # Positional argument or keyword argument given as positional
            #         if arg_name not in arg_kwonlyargs:
            #             arg_dict[arg_name] = args[arg_position]
            #         else:
            #             raise ValueError(
            #                 "Keyword-only parameter '%s' was passed as "
            #                 'positional parameter for %s:\n'
            #                 '     %s was called.'
            # % (arg_name,
            #    _signature_str(name, arg_sig),
            #    _function_called_str(name, args, kwargs))
            # )

            else:
                position = arg_position - len(arg_names)
                if arg_name in kwargs:
                    arg_dict[arg_name] = kwargs[arg_name]
                else:
                    try:
                        arg_dict[arg_name] = arg_defaults[position]
                    except (IndexError, KeyError) as e:
                        pass
                        # Missing argument
                        # raise ValueError(
                        #     'Wrong number of arguments for %s:\n'
                        #     '     %s was called.'
                        #     # % (_signature_str(name, arg_sig),
                        #     #    _function_called_str(name, args, kwargs))
                        # ) from e

        varkwargs = dict()
        for arg_name, arg_value in sorted(kwargs.items()):
            if arg_name in arg_dict:
                arg_dict[arg_name] = arg_value
            elif arg_varkw is not None:
                varkwargs[arg_name] = arg_value
            else:
                raise TypeError(
                    "Ignore list for %s() contains an unexpected "
                    "keyword argument '%s'" % (name, arg_name)
                )

        if arg_varkw is not None:
            arg_dict["**"] = varkwargs
        if arg_varargs is not None:
            varargs = args[arg_position + 1 :]
            arg_dict["*"] = varargs

        return arg_dict
