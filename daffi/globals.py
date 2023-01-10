import os
import sys
import logging
from dataclasses import dataclass, field
from cached_property import cached_property
from inspect import iscoroutinefunction, signature
from threading import Event
from typing import (
    Callable,
    Any,
    Optional,
    Generic,
    List,
    Dict,
    ClassVar,
    Union,
    NoReturn,
    Coroutine,
)
from anyio import sleep
from tenacity import retry, retry_if_exception_type, wait_fixed, stop_after_attempt

from daffi.utils import colors
from daffi.utils.logger import patch_logger
from daffi.exceptions import InitializationError, GlobalContextError
from daffi.ipc import Ipc
from daffi.remote_call import LazyRemoteCall
from daffi.utils.misc import Singleton, is_lambda_function, string_uuid
from daffi.utils.custom_types import GlobalCallback, P
from daffi.callback_types import RemoteClassCallback, RemoteCallback
from daffi.utils.func_validation import (
    get_class_methods,
    func_info,
    pretty_callbacks,
    is_class_or_static_method,
)
from daffi.utils.settings import (
    LOCAL_CALLBACK_MAPPING,
    LOCAL_CLASS_CALLBACKS,
    WELL_KNOWN_CALLBACKS,
)

logger = patch_logger(logging.getLogger(__name__), colors.grey)

__all__ = ["Global", "callback"]


@dataclass
class Global(metaclass=Singleton):
    """
    Main daffi entrypoint for all remote operations eg.
    call remote callbacks, wait for remote processes, obtain additional information from remote etc.
    Args:
       process_name: Global process name. If specified it is used as reference key to Node process.
           By default randomly generated hash is used as reference.
       init_controller: Flag that indicates whether `Controller` should be instantiated in current process
       init_node: Flag that indicates whether `Node` should be instantiated in current process
       host: host to connect `Controller`/`Node` via tcp. If not provided then Global consider UNIX socket connection to be used.
       port: Optional port to connect `Controller`/`Node` via tcp. If not provided random port will be chosen.
       unix_sock_path: Folder where UNIX socket will be created. If not provided default path is < tmp directory >/dafi/
          where `<tmp directory >` is default temporary directory on system.
       reconnect_freq: Optional interval in seconds after which `Node` that is running in current process
            will be re-connected to `Controller`. Daffi uses grpc bidirectional streams to communicate between processes
            They need to keep connection alive all the time by default. It is not the case for some applications
            running behind nginx proxy server or other similar proxies where connection timeout can be specified.
            For such situation this option might help for long running daffi application.
    """

    process_name: Optional[str] = field(default_factory=string_uuid)
    init_controller: Optional[bool] = False
    init_node: Optional[bool] = True
    host: Optional[str] = None
    port: Optional[int] = None
    unix_sock_path: Optional[os.PathLike] = None
    reconnect_freq: Optional[int] = None

    _inside_callback_context: Optional[bool] = field(repr=False, default=False)


    def __post_init__(self):
        self.process_name = str(self.process_name)

        if not (self.init_controller or self.init_node):
            raise InitializationError(
                "No components were found in current process."
                " Provide at least one required argument"
                " `init_controller=True` or `init_node=True`."
            )

        if self.unix_sock_path and self.host:
            raise InitializationError(
                "Provide either 'unix_sock_path' argument or combination "
                "of 'host' and 'port' to connect via unix socket or via tcp respectively"
            )

        if not self.host and sys.platform == "win32":
            raise InitializationError("Windows platform doesn't support unix sockets. Provide host and port to use TCP")

        self._global_terminate_event = Event()
        self.ipc = Ipc(
            process_name=self.process_name,
            init_controller=self.init_controller,
            init_node=self.init_node,
            global_terminate_event=self._global_terminate_event,
            host=self.host,
            port=self.port,
            unix_sock_path=self.unix_sock_path,
            reconnect_freq=self.reconnect_freq,
            logger=logger,
        )

        callback._ipc = self.ipc
        self.ipc.start()
        if not self.ipc.wait():
            raise GlobalContextError("Unable to start daffi components.")
        self.port = self.ipc.port

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        if exc_type is not None:
            return False

    @cached_property
    def call(self) -> LazyRemoteCall:
        """Returns instance of `LazyRemoteCall` that is used to trigger remote callback."""
        return LazyRemoteCall(
            _ipc=self.ipc,
            _global_terminate_event=self._global_terminate_event,
            _inside_callback_context=self._inside_callback_context,
        )

    @property
    def is_controller(self) -> bool:
        """Return True if controller is running in current process"""
        return bool(self.ipc.controller)

    @property
    def registered_callbacks(self) -> Dict[str, List[str]]:
        """Return list of all registered callbacks along with process names where these callbacks are registered."""
        return pretty_callbacks(mapping=self.ipc.node_callback_mapping, exclude_proc=self.process_name, format="dict")

    def join(self) -> NoReturn:
        """
        Join global to main thread.
        Don't use this method if you're running asynchronous application as it blocks event loop.
        """
        return self.ipc.join()

    async def join_async(self):
        """Async version of .join() method. Use this method if your application is asynchronous."""
        while self.ipc.is_alive():
            await sleep(0.5)

    def stop(self, kill_all_connected_nodes: Optional[bool] = False):
        """
        Stop all components (Node/Controller) that is running are current process
        Args:
            kill_all_connected_nodes: Optional flag that indicated whether kill signal should be sent to all connected
                nodes. This argument works only for those processes where controller is running.
        """
        res = None
        if kill_all_connected_nodes:
            if not self.is_controller:
                logger.error("You can kill all nodes only from a process that has a controller")
            else:
                res = self.kill_all()
        self.ipc.stop()
        return res

    def kill_all(self):
        """Kill all connected nodes. This method works only for those processes where controller is running"""
        res = self.ipc.kill_all()
        return res

    def transfer_and_call(
        self, remote_process: str, func: Callable[..., Any], *args, **kwargs
    ) -> Union[Coroutine, Any]:
        """
        Send function along with arguments to execute on remote process.
        This method has some limitations. For example you should import modules inside function
        as modules might be unavailable on remote.
        Args:
            remote_process: Name of node where function should be executed
            args: Any positional arguments to execute function with.
            kwargs: Any keyword arguments to execute function with.
        Example:
            import os

            from daffi import Global

            async def get_remote_pid():
            import os
            return os.getpid()

            g = Global()
            g.transfer_and_call("node_name", get_remote_pid)
        """
        if self._inside_callback_context:
            return self.ipc.async_transfer_and_call(remote_process, func, *args, **kwargs)
        return self.ipc.transfer_and_call(remote_process, func, *args, **kwargs)

    def wait_function(self, func_name: str) -> NoReturn:
        """
        Wait particular remote callback by name to be available.
        This method is useful when callback with the same name is registered on different nodes and you need
        at leas one node to be available.
        Args:
            func_name: Name of remote callback to wait.
        """
        return LazyRemoteCall(_ipc=self.ipc, _global_terminate_event=None, _func_name=func_name)._wait_function()

    def wait_process(self, process_name: str) -> NoReturn:
        """
        Wait particular Node to be alive.
        Args:
            process_name: Name of `Node`
        """
        return LazyRemoteCall(_ipc=self.ipc, _global_terminate_event=None)._wait_process(process_name)

    def get_scheduled_tasks(self, remote_process: str):
        """
        Get all scheduled tasks that are running at this moment on remote process
        Args:
            process_name: Name of `Node`
        """
        return self.ipc.get_all_scheduled_tasks(remote_process=remote_process)

    def cancel_scheduled_task_by_uuid(self, remote_process: str, uuid: int) -> NoReturn:
        """
        Cancel scheduled task by its uuid on remote process.
        Find out task uuid you can using `get_scheduled_tasks` method.
        """
        return self.ipc.cancel_scheduler(remote_process=remote_process, msg_uuid=uuid)


class callback(Generic[GlobalCallback]):
    """
    callback is uniform decorator for registering remote callbacks from functions or from classes
    Example:
        >>> from daffi import callback
        >>>
        >>> @callback
        >>> def my_func(*args, **kwargs):
                ...

    callback works with classes but some limitations are present:

        1. Only `static` methods and `class` methods
            can be triggered without class initialization.
            For all other methods to be instance of class should be instantiated.
        2. Only one instance of class can be instantiated.
        3. Only publicly available methods become callback (means methods which name doensn't start with underscore)
    """
    _ipc: ClassVar[Ipc] = None

    def __init__(self, fn: Callable[P, Any]):

        self._klass = self._fn = None
        if isinstance(fn, type):
            # Class wrapped
            self._klass = fn
            for method in get_class_methods(fn):
                _, name = func_info(method)

                if name.startswith("_"):
                    continue

                fn_type = is_class_or_static_method(fn, name)
                klass = fn if fn_type else None
                cb = RemoteClassCallback(
                    klass=klass,
                    klass_name=fn.__name__,
                    origin_name=name,
                    signature=signature(method),
                    is_async=iscoroutinefunction(method),
                    is_static=str(fn_type) == "static",
                )
                cb.validate_g_position_type()
                LOCAL_CALLBACK_MAPPING[name] = cb
                logger.info(f"{name!r} registered" + ("" if klass else f" (required {fn.__name__} initialization)"))

                if self._ipc and self._ipc.is_running:
                    # Update remote callbacks if ips is running. It means callback was not registered during handshake
                    # or callback was added dynamically.
                    self._ipc.update_callbacks(LOCAL_CALLBACK_MAPPING)

        elif callable(fn):
            if is_lambda_function(fn):
                raise InitializationError("Lambdas is not supported.")

            _, name = func_info(fn)
            self._fn = RemoteCallback(
                callback=fn,
                origin_name=name,
                signature=signature(fn),
                is_async=iscoroutinefunction(fn),
            )
            self._fn.validate_g_position_type()
            LOCAL_CALLBACK_MAPPING[name] = self._fn
            if name not in WELL_KNOWN_CALLBACKS:
                logger.info(f"{name!r} registered")
            if self._ipc and self._ipc.is_running:
                # Update remote callbacks if ips is running. It means callback was not registered during handshake
                # or callback was added dynamically.
                self._ipc.update_callbacks(LOCAL_CALLBACK_MAPPING)

        else:
            raise InitializationError(f"Invalid type. Provide class or function.")

    def __call__(self, *args, **kwargs) -> object:
        if self._klass:
            return self._build_class_callback_instance(*args, **kwargs)
        return self._fn(*args, **kwargs)

    def __getattr__(self, item):
        if self._fn:
            return getattr(self._fn, item)
        else:
            try:
                return LOCAL_CALLBACK_MAPPING[item]
            except KeyError:
                return getattr(self._klass, item)

    def _build_class_callback_instance(self, *args, **kwargs):
        if isinstance(self._klass, type):
            class_name = self._klass.__name__
        else:
            class_name = self._klass.__class__.__name__
        if class_name in LOCAL_CLASS_CALLBACKS:
            raise InitializationError(f"Only one callback instance of {class_name!r} should be created.")

        LOCAL_CLASS_CALLBACKS.add(self._klass.__name__)
        self._klass = self._klass(*args, **kwargs)
        for method in get_class_methods(self._klass):
            module, name = func_info(method)
            if name.startswith("_"):
                continue

            info = LOCAL_CALLBACK_MAPPING.get(name)
            if info:
                LOCAL_CALLBACK_MAPPING[name] = info._replace(klass=self._klass)
        return self


# ----------------------------------------------------------------------------------------------------------------------
# Well known callbacks
# ----------------------------------------------------------------------------------------------------------------------


@callback
def __transfer_and_call(func: Callable[..., Any], *args, **kwargs) -> Any:
    return func(*args, **kwargs)


@callback
async def __async_transfer_and_call(func: Callable[..., Any], *args, **kwargs) -> Any:
    return await func(*args, **kwargs)


@callback
@retry(
    wait=wait_fixed(0.1),
    stop=stop_after_attempt(7),
    retry=retry_if_exception_type(
        RuntimeError,
    ),
    reraise=True,
)
async def __cancel_scheduled_task(msg_uuid: str, process_name: str, func_name: Optional[str] = None) -> bool:
    from daffi.components.scheduler import (
        logger,
        SCHEDULER_PERIODICAL_TASKS,
        SCHEDULER_AT_TIME_TASKS,
        FINISHED_TASKS,
        TaskIdent,
    )

    task_found = False
    if msg_uuid and func_name and process_name:
        task_ident = TaskIdent(process_name, func_name, msg_uuid)
        period_task = SCHEDULER_PERIODICAL_TASKS.pop(task_ident, None)
        if period_task:
            period_task.cancel()
            FINISHED_TASKS.append(task_ident.msg_uuid)
            logger.warning(f"Task {func_name!r} (condition=period, executor={process_name}) has been canceled.")
            task_found = True
        else:
            at_time_tasks = SCHEDULER_AT_TIME_TASKS.pop(task_ident, None)
            if at_time_tasks:
                for at_time_task in at_time_tasks:
                    if at_time_task is not None:
                        at_time_task.cancel()
                        FINISHED_TASKS.append(task_ident.msg_uuid)
                logger.warning(
                    f"Task group {func_name!r} (condition=period, executor={process_name}) has been canceled."
                )
                task_found = True

    elif msg_uuid:
        for task_ident, period_task in SCHEDULER_PERIODICAL_TASKS.items():
            if task_ident.msg_uuid == msg_uuid:
                period_task.cancel()
                SCHEDULER_PERIODICAL_TASKS.pop(task_ident, None)
                FINISHED_TASKS.append(msg_uuid)
                logger.warning(
                    f"Task {task_ident.func_name!r} (condition=period, executor={task_ident.process_name}) has been canceled."
                )
                task_found = True
                break
        else:
            for task_ident, at_time_tasks in SCHEDULER_AT_TIME_TASKS.items():
                if task_ident.msg_uuid == msg_uuid and at_time_tasks:
                    for at_time_task in at_time_tasks:
                        if at_time_task is not None:
                            at_time_task.cancel()
                    SCHEDULER_AT_TIME_TASKS.pop(task_ident, None)
                    FINISHED_TASKS.append(msg_uuid)
                    logger.warning(
                        f"Task group {task_ident.func_name!r} (condition=period, executor={task_ident.process_name}) has been canceled."
                    )
                    task_found = True
                    break
    if not task_found:
        if msg_uuid in FINISHED_TASKS:
            return False
        raise GlobalContextError(f"Unable to find task by uuid: {msg_uuid}")
    return True


@callback
@retry(
    wait=wait_fixed(0.1),
    stop=stop_after_attempt(7),
    retry=retry_if_exception_type(
        RuntimeError,
    ),
    reraise=True,
)
async def __get_all_period_tasks(process_name: str) -> List[Dict]:
    from daffi.components.scheduler import SCHEDULER_PERIODICAL_TASKS, SCHEDULER_AT_TIME_TASKS

    res = []
    for task_ident in SCHEDULER_PERIODICAL_TASKS:
        if task_ident.process_name == process_name:
            res.append(
                {
                    "condition": "period",
                    "func_name": task_ident.func_name,
                    "uuid": task_ident.msg_uuid,
                }
            )

    for task_ident, at_time_tasks in SCHEDULER_AT_TIME_TASKS.items():
        if task_ident.process_name == process_name:
            res.append(
                {
                    "condition": "at_time",
                    "func_name": task_ident.func_name,
                    "uuid": task_ident.msg_uuid,
                    "number_of_active_tasks": list(filter(None, at_time_tasks)),
                }
            )
    return res


@callback
async def __kill_all() -> NoReturn:
    pass
