import os
import time
import sys
import logging
import asyncio
from pathlib import Path
from abc import abstractmethod
from cached_property import cached_property
from typing import Optional, NoReturn, Callable, ClassVar, List
from contextlib import asynccontextmanager
from tempfile import gettempdir

from grpc import aio, ChannelConnectivity
from anyio.abc import TaskStatus
from grpc.aio._call import AioRpcError
from anyio import Event, to_thread, TASK_STATUS_IGNORED
from tenacity import AsyncRetrying, wait_fixed, retry_if_exception_type, RetryCallState, wait_none

from daffi.exceptions import InitializationError, ReckAcceptError, StopComponentError
from daffi.interface import ComponentI
from daffi.components.proto import messager_pb2_grpc as grpc_messager
from daffi.utils.misc import string_uuid


class UnixBase(ComponentI, grpc_messager.MessagerServiceServicer):

    SOCK_FILE = ".sock"

    def __init__(self, unix_sock_path: Optional[os.PathLike]):
        self.unix_sock_path = unix_sock_path

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
        async with aio.insecure_channel(self.unix_socket) as aio_channel:
            yield aio_channel

    async def create_listener(self) -> NoReturn:
        server = aio.server()
        grpc_messager.add_MessagerServiceServicer_to_server(self, server)
        server.add_insecure_port(self.unix_socket)
        await server.start()
        return server


class TcpBase(ComponentI, grpc_messager.MessagerServiceServicer):
    def __init__(self, host: str, port: Optional[int]):
        self.host = host
        self.port = port
        if self.host in ("localhost", "0.0.0.0", "127.0.0.1", "192.168.0.1"):
            self.host = "[::]"

    def info(self) -> str:
        return f"tcp: [ host {self.host!r}, port: {self.port!r} ]"

    @asynccontextmanager
    async def connect_listener(self):
        async with aio.insecure_channel(f"{self.host}:{self.port}") as aio_channel:
            yield aio_channel

    async def create_listener(self, task_status: TaskStatus = TASK_STATUS_IGNORED) -> NoReturn:
        if not self.port:
            await self.find_random_port()
        server = aio.server()
        grpc_messager.add_MessagerServiceServicer_to_server(self, server)
        listen_addr = f"{self.host}:{self.port}"
        server.add_insecure_port(listen_addr)
        await server.start()
        task_status.started()
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
        for port in range(min_port, max_port):
            self.port = port
            if not await self.check_endpoint_is_busy():
                break


class ComponentsBase(UnixBase, TcpBase):
    RETRY_TIMEOUT = 2  # 2 sec
    IMMEDIATE_ACTION_ERRORS = (ReckAcceptError,)
    NON_IMMEDIATE_ACTION_ERRORS = (AioRpcError, StopComponentError)

    logger: logging.Logger = None
    global_terminate_event: ClassVar[Event] = None
    stop_callbacks: ClassVar[List[Callable]] = []

    def __init__(
        self,
        process_name: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        unix_sock_path: Optional[os.PathLike] = None,
        reconnect_freq: Optional[int] = None,
        async_backend: Optional[str] = None,
    ):
        self.process_name = process_name
        self.reconnect_freq = reconnect_freq
        self.async_backend = async_backend or "asyncio"
        self.operations = None
        self.ident = string_uuid()
        self._stopped = self._connected = False

        if host:  # Check only host. Full host/port validation already took place before.
            if self.reconnect_freq and reconnect_freq < 30:
                raise InitializationError(
                    "Too little reconnect frequency was specified."
                    " Specify value for 'reconnect_freq' argument greater than one minute."
                )

            self._base = TcpBase
            self._base.__init__(self, host, port)
        else:
            self.reconnect_freq = None
            self._base = UnixBase
            self._base.__init__(self, unix_sock_path)

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
        self._stopped = True

    @cached_property
    def info(self) -> str:
        return self._base.info(self)

    @asynccontextmanager
    async def connect_listener(self):
        async with self._base.connect_listener(self) as stream:
            self._connected = True
            yield stream

    async def create_listener(self):
        if await self.check_endpoint_is_busy():
            self.logger.error(f"{self.info} is already allocated")
            self._stopped = True
            raise StopComponentError()
        listener = await self._base.create_listener(self)
        self._connected = True
        return listener

    def after_exception(self, retry_state: RetryCallState):
        """return the result of the last call attempt"""

        retry_object = retry_state.retry_object
        attempt = retry_state.attempt_number
        exception = retry_state.outcome.exception()

        if type(exception) in self.IMMEDIATE_ACTION_ERRORS:
            retry_object.begin()
            retry_object.wait = wait_none()

        elif type(exception) == StopComponentError:
            ComponentsBase.global_terminate_event.set()

        elif type(exception) in self.NON_IMMEDIATE_ACTION_ERRORS:
            retry_object.wait = wait_fixed(self.RETRY_TIMEOUT)
            if attempt == 3 or not attempt % 5:
                self.logger.error(
                    f"Unable to connect {self.__class__.__name__}"
                    f" {self.process_name!r}. Error = {type(exception)} {exception}. Retrying..."
                )
        else:
            retry_object.wait = wait_fixed(self.RETRY_TIMEOUT)
            self.logger.error(f"Unpredictable error during {self.__class__.__name__} execution: {exception}")

    async def handle(self, global_terminate_event: Event, task_status: TaskStatus = TASK_STATUS_IGNORED) -> NoReturn:

        if not ComponentsBase.global_terminate_event or ComponentsBase.global_terminate_event.is_set():
            ComponentsBase.global_terminate_event = global_terminate_event
            asyncio.create_task(self.on_global_terminate_event(global_terminate_event))
            self._set_keyboard_interrupt_handler()

        await self.on_init()
        ComponentsBase.stop_callbacks.append(self.on_stop)

        async for attempt in AsyncRetrying(
            reraise=True,
            retry=retry_if_exception_type(
                self.IMMEDIATE_ACTION_ERRORS + self.NON_IMMEDIATE_ACTION_ERRORS + (Exception,)
            ),
            after=self.after_exception,
        ):
            with attempt:
                if self._stopped:
                    return

                await self.before_connect()
                await self.handle_operations(task_status)

    async def on_global_terminate_event(self, global_terminate_event) -> NoReturn:
        await to_thread.run_sync(global_terminate_event.wait)
        for callback in reversed(ComponentsBase.stop_callbacks):
            res = callback()
            if asyncio.iscoroutine(res):
                await res
        ComponentsBase.stop_callbacks.clear()

    def wait(self) -> NoReturn:
        while not self._stopped:
            time.sleep(0.5)

    def _set_keyboard_interrupt_handler(self) -> NoReturn:
        # Creating a handler
        def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                # Will call default excepthook
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
                # Create a critical level log message with info from the except hook.
            self.logger.error("Unhandled exception:", exc_info=(exc_type, exc_value, exc_traceback))

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
