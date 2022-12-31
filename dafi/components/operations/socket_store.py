import logging
from pickle import UnpicklingError
from typing import Optional, Union

import dill
from anyio import EndOfStream, Lock
from anyio.abc._sockets import SocketStream
from anyio import Event, BusyResourceError, sleep, BrokenResourceError, ClosedResourceError

from dafi.message import Message
from dafi.utils.debug import with_debug_trace
from dafi.utils.settings import BYTES_CHUNK

logger = logging.getLogger(__name__)


class SocketPipe:
    def __init__(self, stream: SocketStream, global_terminate_event: Optional[Event] = None):
        self.stream = stream
        self.global_terminate_event = global_terminate_event
        self.lock = Lock()

    @with_debug_trace
    async def send(
        self,
        msg: Message,
        eta: Optional[Union[int, float]] = None,
    ):
        if eta:
            await sleep(eta)

        chunks = msg.dumps()

        async with self.lock:
            attempts = 20
            for chunk in chunks:
                for _ in range(attempts):
                    try:
                        await self.stream.send(chunk)
                        break
                    except BusyResourceError:
                        await sleep(0.1)

                    except (BrokenResourceError, ClosedResourceError):
                        return

    # create an instance of the iterator
    def __aiter__(self):
        return self

    @with_debug_trace
    async def __anext__(self):
        try:
            while not self.global_terminate_event.is_set():
                raw_msg_marker = await self.stream.receive(8)

                if not raw_msg_marker:
                    continue

                raw_data = []

                # TODO use msg_uuid
                msg_uuid, msg_len = Message.get_msg_uuid_and_len_from_bytes(raw_msg_marker)

                # Calculate number of full chunks and rest (last chunk) to take from stream.
                full_chunks, rest = divmod(msg_len, BYTES_CHUNK)
                for chunk in [BYTES_CHUNK] * full_chunks + [rest] if rest else []:
                    packet = await self.stream.receive(chunk)
                    if not packet:
                        return None
                    raw_data.append(packet)
                payload = dill.loads(b"".join(raw_data))
                return Message(*payload)

        except (EndOfStream, ClosedResourceError, BrokenResourceError, UnpicklingError):
            raise StopAsyncIteration

    async def receive(self, max_bytes) -> bytes:
        return await self.stream.receive(max_bytes)

    @with_debug_trace
    async def aclose(self):
        async with self.lock:
            try:
                await self.stream.aclose()
            except Exception:
                ...


class SocketStore(dict):
    @with_debug_trace
    def add_stream(self, stream: SocketStream, process_name: str, global_terminate_event: Optional[Event] = None):
        if isinstance(stream, SocketPipe):
            self[process_name] = stream
        else:
            self[process_name] = SocketPipe(stream=stream, global_terminate_event=global_terminate_event)

    @with_debug_trace
    async def find_process_name_by_stream(self, stream: SocketStream) -> str:
        try:
            return next(proc for proc, sock in list(self.items()) if sock == stream)
        except StopIteration:
            ...

    @with_debug_trace
    async def delete_stream(self, process_name: str):
        if process_name:
            sock = self.get(process_name)
            if sock:
                await sock.aclose()
                self.pop(process_name, None)

    @with_debug_trace
    async def iterate(self):
        for proc_name, sock_pipe in list(self.items()):
            yield proc_name, sock_pipe

    @with_debug_trace
    async def close(self):
        async for _, sock in self.iterate():
            await sock.aclose()
        self.clear()
