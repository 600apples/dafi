import os
import logging
from enum import IntEnum
from functools import cached_property
from typing import Optional, Any
from contextlib import asynccontextmanager

from anyio import BusyResourceError, sleep
from anyio.abc._sockets import SocketListener, UNIXSocketStream, SocketStream
from anyio import create_unix_listener, create_tcp_listener, connect_unix, connect_tcp

from dafi.exceptions import InitializationError
from dafi.interface import ControllerI, NodeI, BackEndI


logger = logging.getLogger(__name__)


async def send_to_stream(stream: SocketStream, item: Any):
    attempts = 20
    for _ in range(attempts):
        try:
            await stream.send(item)
            break
        except BusyResourceError:
            await sleep(0.1)
    else:
        logger.error(f"Failed to send item {item} during {attempts} attempts.")


class ControllerStatus(IntEnum):

    RUNNING = 1
    UNAVAILABLE = 2


class UnixBase(ControllerI, NodeI):

    SOCK_FILE = ".sock"

    def __init__(self, socket_folder):
        self.socket_folder = socket_folder

    def info(self) -> str:
        return f"unix socket: [ {self.unix_socket!r} ]"

    @cached_property
    def unix_socket(self) -> str:
        if not os.path.exists(self.socket_folder):
            raise InitializationError("Socket directory does not exist.")
        return os.path.join(self.socket_folder, self.SOCK_FILE)

    @asynccontextmanager
    async def connect_listener(self) -> UNIXSocketStream:
        async with await connect_unix(self.unix_socket) as stream:
            yield stream

    async def create_listener(self) -> SocketListener:
        if os.path.exists(self.unix_socket):
            os.remove(self.unix_socket)
        return await create_unix_listener(self.unix_socket)


class TcpBase(ControllerI, NodeI):
    def __init__(self, host, port):
        self.host = "0.0.0.0" if host == "localhost" else host
        self.port = port

    def info(self) -> str:
        return f"tcp socket: [ host {self.host!r}, port: {self.port!r} ]"

    @asynccontextmanager
    async def connect_listener(self) -> SocketStream:
        async with await connect_tcp(self.host, self.port) as stream:
            yield stream

    async def create_listener(self) -> SocketListener:
        return await create_tcp_listener(local_port=self.port)


class ComponentsBase(UnixBase, TcpBase):
    TIMEOUT = 6  # 6 sec

    def __init__(
        self,
        process_name: str,
        backend: BackEndI,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ):
        self.backend = backend
        self.process_name = process_name

        if port:
            self._base = TcpBase
            self._base.__init__(self, host, port)
        else:
            self._base = UnixBase
            self._base.__init__(self, backend.base_dir)

    @cached_property
    def info(self) -> str:
        return self._base.info(self)

    @asynccontextmanager
    async def connect_listener(self) -> SocketStream:
        async with self._base.connect_listener(self) as stream:
            yield stream

    async def create_listener(self) -> SocketListener:
        return await self._base.create_listener(self)
