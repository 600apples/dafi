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
    process_name: Optional[str] = field(default_factory=string_uuid)
    init_controller: Optional[bool] = False
    init_node: Optional[bool] = True
    host: Optional[str] = None
    port: Optional[int] = None
    unix_sock_path: Optional[os.PathLike] = None
    reconnect_freq: Optional[int] = None

    _inside_callback_context: Optional[bool] = field(repr=False, default=False)
    """
    Args:
        process_name: Global process name. If specified it is used as reference key for callback response.
            By default randomly generated hash is used as reference.
    """

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
        return LazyRemoteCall(
            _ipc=self.ipc,
            _global_terminate_event=self._global_terminate_event,
            _inside_callback_context=self._inside_callback_context,
        )

    @property
    def is_controller(self) -> bool:
        return bool(self.ipc.controller)

    @property
    def registered_callbacks(self) -> Dict[str, List[str]]:
        return pretty_callbacks(mapping=self.ipc.node_callback_mapping, exclude_proc=self.process_name, format="dict")

    def join(self) -> NoReturn:
        """
        Join global to main thread.
        Don't use this method if you're running asynchronous application as it blocks event loop.
        """
        return self.ipc.join()

    async def join_async(self):
        while self.ipc.is_alive():
            await sleep(0.5)

    def stop(self, kill_all_connected_nodes: Optional[bool] = False):
        res = None
        if kill_all_connected_nodes:
            if not self.is_controller:
                logger.error("You can kill all nodes only from a process that has a controller")
            else:
                res = self.kill_all()
        self.ipc.stop()
        return res

    def kill_all(self):
        res = self.ipc.kill_all()
        return res

    def transfer_and_call(
        self, remote_process: str, func: Callable[..., Any], *args, **kwargs
    ) -> Union[Coroutine, Any]:
        if self._inside_callback_context:
            return self.ipc.async_transfer_and_call(remote_process, func, *args, **kwargs)
        return self.ipc.transfer_and_call(remote_process, func, *args, **kwargs)

    def wait_function(self, func_name: str) -> NoReturn:
        return LazyRemoteCall(_ipc=self.ipc, _global_terminate_event=None, _func_name=func_name)._wait_function()

    def wait_process(self, process_name: str) -> NoReturn:
        return LazyRemoteCall(_ipc=self.ipc, _global_terminate_event=None)._wait_process(process_name)

    def get_scheduled_tasks(self, remote_process: str):
        return self.ipc.get_all_scheduled_tasks(remote_process=remote_process)

    def cancel_scheduled_task_by_uuid(self, remote_process: str, uuid: int) -> NoReturn:
        return self.ipc.cancel_scheduler(remote_process=remote_process, msg_uuid=uuid)


class callback(Generic[GlobalCallback]):
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
