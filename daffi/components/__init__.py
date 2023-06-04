import os
import time
import sys
import logging
import asyncio
import traceback
from pathlib import Path
from random import randint
from abc import abstractmethod
from itertools import count
from cached_property import cached_property
from threading import Event as thEvent, Thread
from typing import Optional, NoReturn, Callable, ClassVar, List, Any, Set
from contextlib import asynccontextmanager
from tempfile import gettempdir


from grpc import aio, ChannelConnectivity, ssl_channel_credentials, ssl_server_credentials
from anyio.abc import TaskStatus
from grpc.aio._call import AioRpcError
from anyio import TASK_STATUS_IGNORED
from tenacity import AsyncRetrying, wait_fixed, retry_if_exception_type, RetryCallState, wait_none

from daffi.exceptions import StopComponentError, ControllerUnavailable, RemoteCallError
from daffi.interface import ComponentI
from daffi.components.proto import messager_pb2_grpc as grpc_messager
from daffi.utils.misc import string_uuid


class UnixBase(ComponentI, grpc_messager.MessagerServiceServicer):
    SOCK_FILE = ".sock"

    def __init__(
        self,
        unix_sock_path: Optional[os.PathLike],
        ssl_certificate: Optional[os.PathLike],
        ssl_key: Optional[os.PathLike],
    ):
        self.unix_sock_path = unix_sock_path
        self.ssl_certificate = ssl_certificate
        self.ssl_key = ssl_key

    def info(self) -> str:
        return f"unix socket: [ {self.unix_socket!r} ]"

    @cached_property
    def base_dir(self) -> os.PathLike:
        _base_dir = self.unix_sock_path or Path(gettempdir()) / "daffi"
        _base_dir.mkdir(parents=True, exist_ok=True)
        return _base_dir

    @cached_property
    def unix_socket(self) -> str:
        self.unix_sock_path = self.base_dir
        sock = "unix:///" + os.path.join(self.base_dir, self.SOCK_FILE).strip("unix:///")
        if os.path.exists(sock):
            os.remove(sock)
        return sock

    @asynccontextmanager
    async def connect_listener(self):
        """UNIX Client"""
        if self.ssl_certificate and self.ssl_key:
            with open(self.ssl_certificate, "rb") as f:
                creds = ssl_channel_credentials(f.read())
            async with aio.secure_channel(self.unix_socket, creds) as aio_channel:
                yield aio_channel
        else:
            async with aio.insecure_channel(self.unix_socket) as aio_channel:
                yield aio_channel

    async def create_listener(self) -> NoReturn:
        """UNIX Server"""
        server = aio.server()
        grpc_messager.add_MessagerServiceServicer_to_server(self, server)
        if self.ssl_certificate and self.ssl_key:
            with open(self.ssl_key, "rb") as f:
                private_key = f.read()
            with open(self.ssl_certificate, "rb") as f:
                certificate_chain = f.read()
            server_credentials = ssl_server_credentials(((private_key, certificate_chain),))
            server.add_secure_port(self.unix_socket, server_credentials)
        else:
            server.add_insecure_port(self.unix_socket)
        await server.start()
        return server


class TcpBase(ComponentI, grpc_messager.MessagerServiceServicer):
    def __init__(
        self, host: str, port: Optional[int], ssl_certificate: Optional[os.PathLike], ssl_key: Optional[os.PathLike]
    ):
        self.host = host
        self.port = port
        self.ssl_certificate = ssl_certificate
        self.ssl_key = ssl_key
        if self.host in ("localhost", "0.0.0.0", "127.0.0.1", "192.168.0.1"):
            self.host = "[::]"

    def info(self) -> str:
        return f"tcp: [ host {self.host!r}, port: {self.port!r} ]"

    @asynccontextmanager
    async def connect_listener(self):
        """TCP Client"""
        listen_addr = f"{self.host}:{self.port}"
        if self.ssl_certificate and self.ssl_key:
            with open(self.ssl_certificate, "rb") as f:
                creds = ssl_channel_credentials(f.read())
            async with aio.secure_channel(listen_addr, creds) as aio_channel:
                yield aio_channel
        else:
            async with aio.insecure_channel(listen_addr) as aio_channel:
                yield aio_channel

    async def create_listener(self) -> NoReturn:
        """TCP Server"""
        if not self.port:
            await self.find_random_port()
        server = aio.server()
        grpc_messager.add_MessagerServiceServicer_to_server(self, server)
        listen_addr = f"{self.host}:{self.port}"
        if self.ssl_certificate and self.ssl_key:
            with open(self.ssl_key, "rb") as f:
                private_key = f.read()
            with open(self.ssl_certificate, "rb") as f:
                certificate_chain = f.read()
            server_credentials = ssl_server_credentials(((private_key, certificate_chain),))
            server.add_secure_port(listen_addr, server_credentials)
        else:
            server.add_insecure_port(listen_addr)
        await server.start()
        return server

    async def find_random_port(self, min_port: Optional[int] = 49152, max_port: Optional[int] = 65536) -> NoReturn:
        """
        Bind this controller to a random port in a range.
        If the port range is unspecified, the system will choose the port.
        Args:
            min_port : int, optional
                The minimum port in the range of ports to try (inclusive).
            max_port : int, optional
                The maximum port in the range of ports to try (exclusive).
        """
        while True:
            self.port = randint(min_port, max_port)
            if not await self.check_endpoint_is_busy():
                break


class ComponentsBase(UnixBase, TcpBase):
    RETRY_TIMEOUT = 1  # 1 sec
    IMMEDIATE_ACTION_ERRORS = (AioRpcError,)
    NON_IMMEDIATE_ACTION_ERRORS = (AioRpcError, RuntimeError, ControllerUnavailable)
    STOP_ACTION_ERRORS = (StopComponentError,)

    logger: logging.Logger = None
    components: ClassVar[Set[Callable]] = set()

    def __init__(
        self,
        g: "Global",
        process_name: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        unix_sock_path: Optional[os.PathLike] = None,
        ssl_certificate: Optional[os.PathLike] = None,
        ssl_key: Optional[os.PathLike] = None,
        async_backend: Optional[str] = None,
        global_terminate_event: Optional[thEvent] = None,
        on_node_connect: Optional[Callable[["Global", str], Any]] = None,
        on_node_disconnect: Optional[Callable[["Global", str], Any]] = None,
    ):
        self._set_keyboard_interrupt_handler()

        self.g = g
        self.process_name = process_name
        self.async_backend = async_backend or "asyncio"
        self.operations = None
        self.ident = string_uuid()
        self.global_terminate_event = global_terminate_event
        self.on_node_connect_cbs = [self.on_node_connect, on_node_connect]
        self.on_node_disconnect_cbs = [self.on_node_disconnect, on_node_disconnect]
        self._stopped = self._connected = False
        self.stop_event: bool = False
        self.stop_callbacks: List[Callable] = []

        self.components.add(self)

        if host:  # Check only host. Full host/port validation already took place before.
            self._base = TcpBase
            self._base.__init__(self, host, port, ssl_certificate, ssl_key)
        else:
            self._base = UnixBase
            self._base.__init__(self, unix_sock_path, ssl_certificate, ssl_key)

    def __hash__(self):
        return hash(self.__class__.__name__)

    @abstractmethod
    async def on_init(self) -> NoReturn:
        raise NotImplementedError

    @abstractmethod
    async def handle_operations(self, task_status: TaskStatus) -> NoReturn:
        raise NotImplementedError

    @abstractmethod
    async def before_connect(self) -> NoReturn:
        raise NotImplementedError

    async def on_stop(self) -> NoReturn:
        while not self.stop_event:
            await asyncio.sleep(0.3)

    def on_node_connect(self, _, process_name: str):
        """
        Default on node connect event handler.
        executed each time when connection to Controller is established
        """
        self.logger.info(f"Node {process_name!r} has been connected to Controller")

    def on_node_disconnect(self, _, process_name: str):
        """
        Default on node disconnect event handler.
        executed each time when connection to Controller is lost
        """
        self.logger.info(f"Node {process_name!r} has been disconnected")

    async def on_terminate(self):
        self.logger.info(f"{self.__class__.__name__} stopped.")
        self._stopped = True
        self.components.discard(self)
        if not self.components:
            # Cancel all existing asyncio tasks if component is the last one in termination chain. Eg if controller
            # is running along with node then controller will be in charge of cleaning.
            self.logger.debug("Cleaning asyncio tasks...")
            tasks = [task for task in asyncio.all_tasks(asyncio.get_running_loop()) if not task.done()]
            for task in tasks:
                task.cancel()

    @abstractmethod
    def on_error(self, exception: Exception) -> NoReturn:
        raise NotImplementedError

    @cached_property
    def info(self) -> str:
        return self._base.info(self)

    def stop(self, wait: Optional[bool] = False):
        self.stop_event = True
        if wait:
            for ind in count(1):

                if self._stopped:
                    break
                if not ind % 100:
                    self.logger.error("Too many iterations of stop event waiting!!!")
                time.sleep(0.5)

    @asynccontextmanager
    async def connect_listener(self):
        async with self._base.connect_listener(self) as stream:
            self._connected = True
            yield stream

    async def create_listener(self):
        if await self.check_endpoint_is_busy():
            self.logger.error(f"{self.info} is already allocated")
            raise StopComponentError()
        listener = await self._base.create_listener(self)
        self._connected = True
        return listener

    async def execute_on_connect(self):
        """Execute `on_connect` lifecycle handler in separate thread to prevent blocking main message handler."""

        def _on_connect_wrapper():
            # In case IPC is not ready yet.
            self.g.ipc.global_condition_event.wait_any_status()
            for cb in filter(None, self.on_node_connect_cbs):
                try:
                    cb(self.g, self.process_name)
                except Exception as e:
                    self.logger.error(
                        f"Exception occurred while execution `on_node_connect` handler: {e}, type: {type(e)}"
                    )

        Thread(target=_on_connect_wrapper).start()

    async def execute_on_disconnect(self, process_name):
        def _on_disconnect_wrapper():
            for cb in filter(None, self.on_node_disconnect_cbs):
                try:
                    cb(self.g, process_name)
                except Exception as e:
                    self.logger.error(
                        f"Exception occurred while execution `on_node_disconnect` handler: {e}, type: {type(e)}"
                    )

        Thread(target=_on_disconnect_wrapper).start()

    def after_exception(self, retry_state: RetryCallState):
        """return the result of the last call attempt"""

        retry_object = retry_state.retry_object
        attempt = retry_state.attempt_number
        exception = retry_state.outcome.exception()
        self.on_error(exception)

        if type(exception) in self.IMMEDIATE_ACTION_ERRORS:
            if type(exception) == AioRpcError:
                if "Deadline Exceeded" in str(exception):
                    retry_object.begin()
                    retry_object.wait = wait_none()
                    return

        elif type(exception) in self.STOP_ACTION_ERRORS:
            self.logger.debug("Perform stop action")
            self.stop()
            self.check_stopped_components()
            return

        if type(exception) in self.NON_IMMEDIATE_ACTION_ERRORS:
            retry_object.wait = wait_fixed(self.RETRY_TIMEOUT)
            if attempt == 3 or not attempt % 6:
                self.logger.error(
                    f"Unable to connect {self.__class__.__name__}"
                    f" {self.process_name!r}. Error = {type(exception)} {exception}. Retrying..."
                )
        else:
            retry_object.wait = wait_fixed(self.RETRY_TIMEOUT)
            self.logger.error(f"Unpredictable error during {self.__class__.__name__} execution")
            traceback.print_tb(exception.__traceback__)
            self.stop()
            self.check_stopped_components()

    def check_stopped_components(self):
        if all(c.stop_event for c in self.components) and self.global_terminate_event:
            self.logger.debug("All components are stopped. Set global terminate event.")
            self.global_terminate_event.set()

    async def handle(self, task_status: TaskStatus = TASK_STATUS_IGNORED) -> NoReturn:
        await self.on_init()

        async for attempt in AsyncRetrying(
            reraise=True,
            retry=retry_if_exception_type((Exception,)),
            after=self.after_exception,
        ):
            with attempt:
                if self.stop_event or self.global_terminate_event.is_set():
                    break

                await self.before_connect()

                stop_task = asyncio.create_task(self.on_stop())
                try:
                    await asyncio.gather(self.handle_operations(task_status), stop_task)
                finally:
                    stop_task.cancel()

        await self.on_terminate()

    def _set_keyboard_interrupt_handler(self) -> NoReturn:
        # Creating a handler
        def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                # Will call default excepthook
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
                # Create a critical level log message with info from the except hook.
            self.logger.error("Unhandled exception:", exc_info=(exc_type, exc_value, exc_traceback))
            for component in self.components:
                component.stop()
            self.check_stopped_components()

        # Assign the excepthook to the handler
        sys.excepthook = handle_unhandled_exception

    async def check_endpoint_is_busy(self) -> bool:
        """Check if unix socket/host-port is already allocated"""
        async with self.connect_listener() as channel:
            for _ in range(30):
                state = channel.get_state(try_to_connect=True)
                if state == ChannelConnectivity.TRANSIENT_FAILURE:
                    # Ready to connect
                    return False
                elif state == ChannelConnectivity.READY:
                    return True
                await asyncio.sleep(0.3)
