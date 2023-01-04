import os
import logging
import asyncio
from cached_property import cached_property
from typing import Optional, NoReturn, Callable, ClassVar, List
from contextlib import asynccontextmanager

from grpc import aio
from anyio.abc import TaskStatus
from grpc.aio._call import AioRpcError
from anyio import Event, to_thread, TASK_STATUS_IGNORED
from tenacity import AsyncRetrying, wait_fixed, retry_if_exception_type, RetryCallState, wait_none

from dafi.exceptions import InitializationError, ReckAcceptError
from dafi.interface import ComponentI, BackEndI
from dafi.components.proto import messager_pb2_grpc as grpc_messager


class UnixBase(ComponentI, grpc_messager.MessagerServiceServicer):

    SOCK_FILE = ".sock"

    def __init__(self, socket_folder):
        self.socket_folder = socket_folder

    def info(self) -> str:
        return f"unix socket: [ {self.unix_socket!r} ]"

    @cached_property
    def unix_socket(self) -> str:
        if not os.path.exists(self.socket_folder):
            raise InitializationError("Socket directory does not exist.")
        return "unix:///" + os.path.join(self.socket_folder, self.SOCK_FILE).strip("unix:///")

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
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def info(self) -> str:
        return f"tcp socket: [ host {self.host!r}, port: {self.port!r} ]"

    @asynccontextmanager
    async def connect_listener(self):
        async with aio.insecure_channel(f"{self.host}:{self.port}") as aio_channel:
            yield aio_channel

    async def create_listener(self, task_status: TaskStatus = TASK_STATUS_IGNORED) -> NoReturn:
        server = aio.server()
        grpc_messager.add_MessagerServiceServicer_to_server(self, server)
        listen_addr = f"{self.host}:{self.port}"
        server.add_insecure_port(listen_addr)
        await server.start()
        task_status.started()
        return server


class ComponentsBase(UnixBase, TcpBase):
    RETRY_TIMEOUT = 1  # 1 sec
    IMMEDIATE_ACTION_ERRORS = (ReckAcceptError, )
    NON_IMMEDIATE_ACTION_ERRORS = (AioRpcError, )

    logger: logging.Logger = None
    global_terminate_event: ClassVar[Event] = None
    stop_callbacks: ClassVar[List[Callable]] = []

    def __init__(
        self,
        process_name: str,
        backend: BackEndI,
        host: Optional[str] = None,
        port: Optional[int] = None,
        reconnect_freq: Optional[int] = None,
    ):
        self.backend = backend
        self.process_name = process_name
        self.reconnect_freq = reconnect_freq

        self.operations = None

        if port:  # Check only port. Full host/port validation already took place before.
            if self.reconnect_freq and reconnect_freq < 60:
                raise InitializationError(
                    "Too little reconnect frequency was specified."
                    " Specify value for 'reconnect_freq' argument greater than one minute."
                )

            self._base = TcpBase
            self._base.__init__(self, host, port)
        else:
            self.reconnect_freq = None
            self._base = UnixBase
            self._base.__init__(self, backend.base_dir)

    @cached_property
    def info(self) -> str:
        return self._base.info(self)

    @asynccontextmanager
    async def connect_listener(self):
        async with self._base.connect_listener(self) as stream:
            yield stream

    def after_exception(self, retry_state: RetryCallState):
        """return the result of the last call attempt"""

        retry_object = retry_state.retry_object
        attempt = retry_state.attempt_number
        exception = retry_state.outcome.exception()

        if type(exception) in self.IMMEDIATE_ACTION_ERRORS:
            retry_object.begin()
            retry_object.wait = wait_none()

        else:
            retry_object.wait = wait_fixed(self.RETRY_TIMEOUT)
            if attempt == 3 or not attempt % 6:
                self.logger.error(
                    f"Unable to connect {self.__class__.__name__}"
                    f" {self.process_name!r}. Error = {type(exception)} {exception}. Retrying..."
                )

    async def handle(self, global_terminate_event: Event, task_status: TaskStatus = TASK_STATUS_IGNORED) -> NoReturn:
        if not ComponentsBase.global_terminate_event or ComponentsBase.global_terminate_event.is_set():
            ComponentsBase.global_terminate_event = global_terminate_event
            asyncio.create_task(self.on_stop(global_terminate_event))

        async for attempt in AsyncRetrying(
            reraise=True,
            retry=retry_if_exception_type(self.IMMEDIATE_ACTION_ERRORS + self.NON_IMMEDIATE_ACTION_ERRORS),
            after=self.after_exception,
        ):
            with attempt:

                if ComponentsBase.global_terminate_event.is_set():
                    return

                await self._handle(task_status)

    async def create_listener(self):
        return await self._base.create_listener(self)

    async def on_stop(self, global_terminate_event) -> NoReturn:
        await to_thread.run_sync(global_terminate_event.wait)
        for callback in reversed(ComponentsBase.stop_callbacks):
            res = callback()
            if asyncio.iscoroutine(res):
                await res
        ComponentsBase.stop_callbacks.clear()
        ComponentsBase.global_terminate_event = None

    def register_on_stop_callback(self, callback: Callable) -> NoReturn:
        ComponentsBase.stop_callbacks.append(callback)
