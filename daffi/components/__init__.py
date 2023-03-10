import os
import time
import sys
import logging
import asyncio
import traceback
from pathlib import Path
from abc import abstractmethod
from itertools import count
from cached_property import cached_property
from threading import Event as thEvent
from typing import Optional, NoReturn, Callable, ClassVar, List
from contextlib import asynccontextmanager
from tempfile import gettempdir

from grpc import aio, ChannelConnectivity
from anyio.abc import TaskStatus
from grpc.aio._call import AioRpcError
from anyio import TASK_STATUS_IGNORED
from tenacity import AsyncRetrying, wait_fixed, retry_if_exception_type, RetryCallState, wait_none

from daffi.exceptions import StopComponentError
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
        async with aio.insecure_channel(self.unix_socket, options=self.client_options) as aio_channel:
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
        async with aio.insecure_channel(f"{self.host}:{self.port}", options=self.client_options) as aio_channel:
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
    IMMEDIATE_ACTION_ERRORS = (AioRpcError,)
    NON_IMMEDIATE_ACTION_ERRORS = (AioRpcError, RuntimeError)
    STOP_ACTION_ERRORS = (StopComponentError,)

    logger: logging.Logger = None
    components: ClassVar[List[Callable]] = []

    def __init__(
        self,
        process_name: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        unix_sock_path: Optional[os.PathLike] = None,
        async_backend: Optional[str] = None,
        global_terminate_event: Optional[thEvent] = None,
    ):
        self._set_keyboard_interrupt_handler()

        self.process_name = process_name
        self.async_backend = async_backend or "asyncio"
        self.operations = None
        self.ident = string_uuid()
        self.global_terminate_event = global_terminate_event
        self._stopped = self._connected = False
        self.stop_event: bool = False
        self.stop_callbacks: List[Callable] = []

        self.components.append(self)

        if host:  # Check only host. Full host/port validation already took place before.
            self._base = TcpBase
            self._base.__init__(self, host, port)
        else:
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
        while not self.stop_event:
            await asyncio.sleep(0.1)

    @abstractmethod
    def on_error(self) -> NoReturn:
        raise NotImplementedError

    @cached_property
    def client_options(self):
        """Grpc client options"""
        # GRPC_ARG_KEEPALIVE_TIME_MS   "grpc.keepalive_time_ms"
        #   After a duration of this time the client/server pings its peer to see if the transport is still alive.
        # GRPC_ARG_KEEPALIVE_TIMEOUT_MS   "grpc.keepalive_timeout_ms"
        #   After waiting for a duration of this time, if the keepalive ping
        #   sender does not receive the ping ack, it will close the transport.
        return [("grpc.keepalive_timeout_ms", 1000), ("grpc.keepalive_time_ms", 360000)]

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

    def after_exception(self, retry_state: RetryCallState):
        """return the result of the last call attempt"""

        self.on_error()
        retry_object = retry_state.retry_object
        attempt = retry_state.attempt_number
        exception = retry_state.outcome.exception()

        if type(exception) in self.IMMEDIATE_ACTION_ERRORS:
            if type(exception) == AioRpcError:
                if "Cancelling all calls" in str(exception):
                    self.logger.debug("Cancelling all tasks and stop.")
                    self.stop()
                    self.check_stopped_components()
                    return
                elif "Deadline Exceeded" in str(exception) or "Controller is down!" in str(exception):
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
            if attempt == 3 or not attempt % 5:
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
        # Register on_stop actions
        stop_task = asyncio.create_task(self.on_stop())
        await self.on_init()

        async for attempt in AsyncRetrying(
            reraise=True,
            retry=retry_if_exception_type((Exception,)),
            after=self.after_exception,
        ):
            with attempt:
                if self.stop_event:
                    return

                await self.before_connect()
                await self.handle_operations(task_status)

        await stop_task

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

            if all(c.stop_event for c in self.components) and self.global_terminate_event:
                self.global_terminate_event.set()

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
