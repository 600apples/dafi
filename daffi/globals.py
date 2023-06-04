import os
import sys
from dataclasses import dataclass
from threading import Event, Thread
from typing import (
    Union,
    NoReturn,
    Coroutine,
    Tuple,
)
from anyio import sleep
from daffi.utils import colors
from daffi.utils.logger import get_daffi_logger
from daffi.exceptions import InitializationError
from daffi.ipc import Ipc
from daffi.remote_call import LazyRemoteCall
from daffi.utils.misc import Singleton, string_uuid
from daffi.utils.func_validation import pretty_callbacks

# Register well known callbacks
from daffi.well_known_callbacks import *

logger = get_daffi_logger("global", colors.blue)

__all__ = ["Global"]


@dataclass
class Global(metaclass=Singleton):
    """
    The main entry point for all remote operations, such as calling remote callbacks,
     waiting for remote processes, and obtaining additional information from remote sources.
    Args:
       process_name: Global process name. If specified it is used as reference key to Node process.
           By default randomly generated hash is used as reference.
       init_controller: Flag that indicates whether `Controller` should be instantiated in current process
       init_node: Flag that indicates whether `Node` should be instantiated in current process
       host: host to connect `Controller`/`Node` via tcp. If not provided then Global consider UNIX socket connection to be used.
       port: Optional port to connect `Controller`/`Node` via tcp. If not provided random port will be chosen.
       unix_sock_path: Folder where UNIX socket will be created. If not provided default path is < tmp directory >/dafi/
           where `<tmp directory >` is default temporary directory on system.
       on_init: Function that will be executed once when Global object is initialized. `on_init` takes Global object as first argument
       on_node_connect: Function that will be executed each time when connection
           to Controller is established (IOW it works only for Nodes). `on_connect` takes Global object as first argument
       on_node_disconnect: Function that will be executed each time when connection to Controller is lost
    """

    process_name: Optional[str] = None
    init_controller: Optional[bool] = False
    init_node: Optional[bool] = True
    host: Optional[str] = None
    port: Optional[int] = None
    unix_sock_path: Optional[os.PathLike] = None
    ssl_certificate: Optional[os.PathLike] = None
    ssl_key: Optional[os.PathLike] = None

    # Lifecycle callback events
    on_init: Optional[Callable[["Global", str], Any]] = None  # on global init (executed once)
    on_node_connect: Optional[
        Callable[["Global", str], Any]
    ] = None  # on node connect (executed each time node is connected)
    on_node_disconnect: Optional[
        Callable[["Global", str], Any]
    ] = None  # on node disconnect (executed each time node is disconnected)

    def __post_init__(self):
        if self.process_name is None:
            self.process_name = string_uuid()
        self.process_name = str(self.process_name)

        if not (self.init_controller or self.init_node):
            InitializationError(
                "No components were found in current process."
                " Provide at least one required argument"
                " `init_controller=True` or `init_node=True`."
            ).fire()

        if self.unix_sock_path and self.host:
            InitializationError(
                "Provide either 'unix_sock_path' argument or combination "
                "of 'host' and 'port' to connect via unix socket or via tcp respectively"
            ).fire()

        if not self.host and sys.platform == "win32":
            InitializationError(
                "Windows platform doesn't support unix sockets. Provide host and port to use TCP"
            ).fire()

        if self.ssl_certificate and not os.path.exists(self.ssl_certificate):
            InitializationError("ssl_certificate: invalid path").fire()

        if self.ssl_key and not os.path.exists(self.ssl_key):
            InitializationError("ssl_key: invalid path").fire()

        self._global_terminate_event = Event()
        self.ipc = Ipc(
            g=self,
            process_name=self.process_name,
            init_controller=self.init_controller,
            init_node=self.init_node,
            global_terminate_event=self._global_terminate_event,
            host=self.host,
            port=self.port,
            unix_sock_path=self.unix_sock_path,
            ssl_certificate=self.ssl_certificate,
            ssl_key=self.ssl_key,
            on_node_connect=self.on_node_connect,
            on_node_disconnect=self.on_node_disconnect,
            logger=logger,
        )
        self.ipc.start()
        if not self.ipc.wait():
            self.stop()
            logger.error("Unable to start daffi components.")
            return
        self.port = self.ipc.port
        if self.on_init and callable(self.on_init):
            Thread(target=self.on_init, args=(self, self.process_name)).start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        if exc_type is not None:
            return False

    @property
    def call(self) -> LazyRemoteCall:
        """Returns instance of `LazyRemoteCall` that is used to trigger remote callback."""
        return LazyRemoteCall(
            _ipc=self.ipc,
            _global_terminate_event=self._global_terminate_event,
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

    def stop(self):
        """
        Stop all components (Node/Controller) that is running are current process"""
        self.ipc.stop()

    def transfer_and_call(
        self, remote_process: str, func: Callable[..., Any], *args: Tuple[Any], **kwargs: Dict[Any, Any]
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
        return self.ipc.transfer_and_call(remote_process, func, *args, **kwargs)

    def wait_function(self, func_name: str) -> NoReturn:
        """
        Wait particular remote callback by name to be available.
        This method is useful when callback with the same name is registered on different nodes and you need
        at leas one node to be available.
        Args:
            func_name: Name of remote callback to wait.
        """
        logger.info(f"Waiting remote callback: {func_name!r}")
        result = LazyRemoteCall(_ipc=self.ipc, _global_terminate_event=None, _func_name=func_name)._wait_function()
        logger.info(f"Function found: {func_name!r}")
        return result

    async def wait_function_async(self, func_name: str) -> NoReturn:
        """
        Wait particular remote callback by name to be available (async version).
        This method is useful when callback with the same name is registered on different nodes and you need
        at leas one node to be available.
        Args:
            func_name: Name of remote callback to wait.
        """
        logger.info(f"Waiting remote callback: {func_name!r}")
        result = await LazyRemoteCall(_ipc=self.ipc, _global_terminate_event=None, _func_name=func_name)._wait_function(
            _async=True
        )
        logger.info(f"Function found: {func_name!r}")
        return result

    def wait_process(self, process_name: str) -> NoReturn:
        """
        Wait particular Node to be alive.
        Args:
            process_name: Name of `Node`
        """
        logger.info(f"Waiting node: {process_name!r}")
        result = LazyRemoteCall(_ipc=self.ipc, _global_terminate_event=None)._wait_process(process_name)
        logger.info(f"Node found: {process_name!r}")
        return result

    async def wait_process_async(self, process_name: str) -> NoReturn:
        """
        Wait particular Node to be alive (async version).
        Args:
            process_name: Name of `Node`
        """
        logger.info(f"Waiting node: {process_name!r}")
        result = await LazyRemoteCall(_ipc=self.ipc, _global_terminate_event=None)._wait_process(
            process_name, _async=True
        )
        logger.info(f"Node found: {process_name!r}")
        return result

    def get_scheduled_tasks(self, remote_process: str):
        """
        Get all scheduled tasks that are running at this moment on remote process
        Args:
            remote_process: Name of `Node`
        """
        return self.ipc.get_all_scheduled_tasks(remote_process=remote_process)

    def cancel_scheduled_task_by_uuid(self, remote_process: str, uuid: int) -> NoReturn:
        """
        Cancel scheduled task by its uuid on remote process.
        Find out task uuid you can using `get_scheduled_tasks` method.
        """
        return self.ipc.cancel_scheduler(remote_process=remote_process, msg_uuid=uuid)
