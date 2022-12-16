import os
import time
from collections import deque
from functools import cached_property
from typing import NoReturn, Deque, ClassVar, Dict

from anyio.abc._sockets import SocketStream
from anyio import Event, create_task_group, move_on_after, sleep, TASK_STATUS_IGNORED

from dafi.exceptions import InitializationError
from dafi.components import ComponentsBase
from dafi.backend import BackEndKeys
from dafi.messaging import Message, MessageFlag
from dafi.utils import CALLBACK_MAPPING


class UnixComponentBase(ComponentsBase):
    SOCK_FILE = ".sock"

    @cached_property
    def socket(self):
        base_dir = self.backend.base_dir
        if not os.path.exists(base_dir):
            raise InitializationError(
                f"Backend {self.backend} is not suitable for creating unix connections. "
                f"Property 'base_dir' should return the path to a local directory that exists"
            )
        return os.path.join(base_dir, self.SOCK_FILE)


class UnixSocketMaster(UnixComponentBase):
    socket_store: ClassVar[Dict[str, SocketStream]] = dict()

    async def handle(self):
        from anyio import create_unix_listener

        self.start_event = Event()
        if os.path.exists(self.socket):
            os.remove(self.socket)
        listener = await create_unix_listener(self.socket)

        await self.start_event.set()
        await listener.serve(self._handle_commands)

    async def healthcheck(self) -> NoReturn:
        while not self.stop_event.is_set():
            self.backend.write(BackEndKeys.MASTER_HEALTHCHECK_TS, time.time())
            await sleep(self.TIMEOUT / 2)

    async def wait(self, *, task_status=TASK_STATUS_IGNORED):
        async with create_task_group() as sg:
            with move_on_after(self.TIMEOUT) as scope:
                await self.start_event.wait()
            if scope.cancel_called:
                raise InitializationError(f"Master did not start within {self.TIMEOUT} seconds")
            task_status.started("STARTED")
            sg.start_soon(self.healthcheck)

    async def _handle_commands(self, stream: SocketStream):
        async with stream:
            while not self.stop_event.is_set():
                raw_msglen = await stream.receive(4)
                msglen = Message.msglen(raw_msglen)
                msg = Message.loads(await stream.receive(msglen))

                if msg.flag == MessageFlag.HANDSHAKE:
                    if msg.transmitter in self.socket_store:
                        raise InitializationError(f"Detected 2 remote processes with the same name {msg.transmitter}")
                    self.socket_store[msg.transmitter] = stream

                elif msg.flag == MessageFlag.REQUEST:
                    async with create_task_group() as sg:
                        for sock in self.socket_store.values():
                            sg.start_soon(sock.send, msg.dumps())


class UnixSocketNode(UnixComponentBase):
    item_store: ClassVar[Deque] = deque()

    async def handle(self):
        from anyio import connect_unix
        async with await connect_unix(self.socket) as stream:
            async with create_task_group() as sg:
                sg.start_soon(self._read_commands, stream)
                sg.start_soon(self._write_commands, stream)

    async def _read_commands(self, stream: SocketStream):
        while not self.stop_event.is_set():
            raw_msglen = await stream.receive(4)
            msglen = Message.msglen(raw_msglen)
            msg = Message.loads(await stream.receive(msglen))

            if msg.flag == MessageFlag.REQUEST:
                info = CALLBACK_MAPPING.get(msg.func_name)
                print(info)

    async def _write_commands(self, stream: SocketStream):
        self.send(Message(flag=MessageFlag.HANDSHAKE, transmitter=self.process_name).dumps())
        while not self.stop_event.is_set():
            if len(self.item_store):
                item = self.item_store.pop()
                await stream.send(item)
            await sleep(.5)

    def send(self, message: bytes):
        self.item_store.append(message)
