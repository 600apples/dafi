import os
import sys
import socket
import time
from abc import ABC
from enum import IntEnum
from threading import Event
from typing import Union, Optional, List, Callable, Dict, Any
import dfcore
from daffi.utils import colors
from daffi.utils.logger import get_daffi_logger
from daffi.exceptions import InitializationError
from daffi.utils.misc import string_uuid
from daffi.rpc_proxy import RpcProxy, SerdeFormat
from daffi.signals import set_signal_handler
from daffi.task_dispatcher import TaskDispatcher
from daffi.registry.executor import EXECUTOR_REGISTRY


class ServerMode(IntEnum):
    ROUTER = 0
    SERVICE = 1


class Application(ABC):
    server_mode: ServerMode = None
    _task_dispatcher: TaskDispatcher = None

    def __init__(
        self,
        app_name: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        unix_sock_path: Optional[os.PathLike] = None,
    ):
        self.app_name = app_name
        self.host = host
        self.port = port
        self.unix_sock_path = str(unix_sock_path or "")
        self._conn_num = None
        self._conn_type = None
        self._stop_event = Event()
        self.event_handlers: List[Callable[[Dict], Any]] = []

        if self.app_name is None:
            self.app_name = f"{socket.gethostname()}-{string_uuid()}"
        self.app_name = str(self.app_name)
        process_ident = f"{self.__class__.__name__.lower()}[{self.app_name}]"
        if self.server_mode == ServerMode.ROUTER:
            color = colors.blue
        elif self.server_mode == ServerMode.SERVICE:
            color = colors.yellow
        else:
            color = colors.magenta
        self.logger = get_daffi_logger(process_ident, color)

        if self.unix_sock_path and self.host:
            raise InitializationError(
                "Provide either 'unix_sock_path' argument or combination "
                "of 'host' and 'port' to connect via unix socket or via tcp respectively"
            )

        if not self.host and sys.platform == "win32":
            raise (
                "Windows platform doesn't support unix sockets. Provide host and port to use TCP"
            )
        set_signal_handler(self.stop)

    @property
    def info(self) -> str:
        if self.unix_sock_path:
            sock = "unix:///" + self.unix_sock_path.strip("unix:///")
            return f"unix socket: [ {sock!r} ]"
        else:
            return f"tcp: [ host {self.host!r}, port: {self.port!r} ]"

    def _register_executors(self):
        for _, executor in EXECUTOR_REGISTRY:
            self.logger.info(f"{executor} registered.")
        def registry_subscriber(executor):
            if self.server_mode is None:
                RpcProxy._process_client_handshake(self._conn_num)
            elif self.server_mode == ServerMode.SERVICE:
                RpcProxy._process_service_handshake(self._conn_num)
            else:
                return
            self.logger.info(f"{executor} registered.")

        EXECUTOR_REGISTRY.subscribers.append(registry_subscriber)


    def stop(self, *args, **kwargs):
        return super().stop(*args, **kwargs)


class ServerMixin:
    def join(self):
        self._stop_event.wait()

    def start(self, password: str = ""):
        if self._conn_num is not None:
            raise RuntimeError("Router is already started")
        self._conn_num = dfcore.startServer(
            self.host, self.port, self.server_mode, password, self.app_name
        )
        if self._conn_num is None:
            raise InitializationError(
                f"Failed to start the server. connection info: {self.info}"
            )
        self.logger.info(f"has been started successfully. connection info: {self.info}"
        )
        self._register_executors()
        time.sleep(0.005)  # flush the log

    def stop(self, *_, **__):
        if not self._stop_event.is_set():
            if self._conn_num is not None:
                dfcore.stopServer(self._conn_num)
        self._stop_event.set()
        if self._task_dispatcher:
            self._task_dispatcher.stop_for_connection(self)


class Router(Application, ServerMixin):
    server_mode = ServerMode.ROUTER

    def start(self, password: str = ""):
        super().start(password)


class Service(Application, ServerMixin):
    server_mode = ServerMode.SERVICE

    def start(self, password: str = ""):
        super().start(password)
        if not self._task_dispatcher:
            self._task_dispatcher = TaskDispatcher()
        self._task_dispatcher.start_for_connection(self)
        RpcProxy._process_service_handshake(self._conn_num)

    def add_event_handler(self, handler: Callable[[Dict], Any]):
        self.event_handlers.append(handler)


class Client(Application):

    @property
    def info(self) -> str:
        if self.unix_sock_path:
            sock = "unix:///" + self.unix_sock_path.strip("unix:///")
            return f"unix socket: [ {sock!r} ]"
        else:
            return f"tcp: [ host {self.host!r}, port: {self.port!r}, type: {self._conn_type!r} ]"

    def connect(self, password: str = "") -> "ClientConnection":
        if self._conn_num is not None:
            raise RuntimeError("Client is already connected")
        self._conn_num = dfcore.startClient(
            self.host, self.port, password, self.app_name
        )
        if self._conn_num is None:
            raise InitializationError(
                f"Failed to connect to the server. connection info: {self.info}"
            )
        handshake = RpcProxy._process_client_handshake(self._conn_num)
        self._conn_type = handshake["meta"]["type"]
        self.logger.info(
            f"has been connected successfully."
            f" connection info: {self.info}"
        )
        if self._conn_type == "router":
            if not self._task_dispatcher:
                self._task_dispatcher = TaskDispatcher()
            self._task_dispatcher.start_for_connection(self)
            self._register_executors()
        time.sleep(0.005)  # flush the log
        return ClientConnection(self)

    def add_event_handler(self, handler: Callable[[Dict], Any]):
        self.event_handlers.append(handler)

    def stop(self, *_, **__):
        if not self._stop_event.is_set():
            if self._conn_num is not None:
                dfcore.stopClient(self._conn_num)
        self._stop_event.set()
        if self._task_dispatcher:
            self._task_dispatcher.stop_for_connection(self)


class ClientConnection:
    def __init__(self, client: Client):
        self.client = client

    def rpc(
        self,
        timeout: Union[int, None] = None,
        receiver: Union[str, List[str], None] = None,
        serde: SerdeFormat = SerdeFormat.PICKLE,
    ) -> RpcProxy:
        return RpcProxy(
            conn=self,
            timeout=timeout,
            receiver=receiver,
            serde=serde,
            return_result=True,
            logger=self.client.logger,
        )

    def stream(self, receiver: Union[str, List[str], None] = None, serde: SerdeFormat = SerdeFormat.PICKLE):
        return RpcProxy(
            conn=self,
            timeout=None,
            receiver=receiver,
            serde=serde,
            return_result=False,
            logger=self.client.logger,
        )
