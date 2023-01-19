import asyncio
from dataclasses import dataclass, field
from contextlib import contextmanager, asynccontextmanager
from typing import Any, NoReturn, Optional, List, Union, Iterator, Tuple

from daffi.components.operations.freezable_queue import FreezableQueue, ItemPriority, STOP_MARKER


@dataclass
class StreamPair:
    # TODO make abstraction for methods of stream pair and message iterator
    stream_queue: FreezableQueue
    _closed: bool = field(repr=False, default=False)

    # create an instance of the iterator
    async def __aiter__(self):
        async for msg in self.stream_queue.iterate():
            yield msg

    @property
    def closed(self) -> bool:
        return self._closed

    @closed.setter
    def closed(self, new_val):
        self._closed = new_val

    def send_threadsave(self, item: Any, priority: Optional[ItemPriority] = ItemPriority.NORMAL) -> NoReturn:
        self.stream_queue.send_threadsave(item, priority)

    async def send(self, item: Any) -> NoReturn:
        await self.stream_queue.send(item)

    def stop_threadsave(self, priority: Optional[ItemPriority] = ItemPriority.LAST) -> NoReturn:
        self.send_threadsave(STOP_MARKER, priority)

    async def stop(self, priority: Optional[ItemPriority] = ItemPriority.LAST) -> NoReturn:
        await self.stream_queue.stop(priority)

    def freeze(self, timeout: int) -> NoReturn:
        self.stream_queue.freeze(timeout=timeout)

    def proceed(self) -> NoReturn:
        self.stream_queue.proceed()


class StreamPairStore(dict):
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.stream_pairs_queue = asyncio.Queue()

    def create_stream_pair(self, *strings) -> StreamPair:
        key = "-".join(strings)
        stream_pair = StreamPair(stream_queue=FreezableQueue(self.loop))
        self[key] = stream_pair
        return stream_pair

    def get_or_create_stream_pair(self, *strings) -> StreamPair:
        key = "-".join(strings)
        stream_pair = self.get(key)
        if not stream_pair:
            stream_pair = self.create_stream_pair(*strings)
        return stream_pair

    def delete_stream_pair(self, *strings) -> NoReturn:
        key = "-".join(strings)
        stream_pair = self.pop(key, None)
        return stream_pair

    @contextmanager
    def request_multi_connection(self, receivers: Union[str, List[str]], msg_uuid: str) -> NoReturn:
        # Create stream pair group (contains one or more stream pair)
        if isinstance(receivers, str):
            receivers = [receivers]
        stream_pair_group = StreamPairGroup(
            [self.get_or_create_stream_pair(receiver, msg_uuid) for receiver in receivers]
        )
        # Send all stream pairs to side which accepts connection
        asyncio.run_coroutine_threadsafe(
            self.stream_pairs_queue.put((stream_pair_group, receivers, msg_uuid)), self.loop
        ).result()
        try:
            yield stream_pair_group
        finally:
            for stream_pair in stream_pair_group:
                stream_pair.stop_threadsave()
            for receiver in receivers:
                self.delete_stream_pair(receiver, msg_uuid)

    async def accept_multi_connection(self) -> Tuple["StreamPairGroup", str]:
        return await self.stream_pairs_queue.get()

    @asynccontextmanager
    async def request_paired_connection(self, receiver: str, msg_uuid: str, message_iterator: Iterator):
        stream_pair = self.get_or_create_stream_pair(receiver, msg_uuid)
        await stream_pair.send(message_iterator)
        try:
            yield stream_pair.stream_queue.wait
        finally:
            await stream_pair.stop()
            self.delete_stream_pair(receiver, msg_uuid)

    @asynccontextmanager
    async def accept_paired_connection(self, receiver: str, msg_uuid: str):
        stream_pair = self.get_or_create_stream_pair(receiver, msg_uuid)
        data = await stream_pair.stream_queue.get()
        try:
            yield data.data
        finally:
            stream_pair.stream_queue.reset()


class StreamPairGroup(List[StreamPair]):
    @property
    def closed(self) -> bool:
        return any(sp.closed for sp in self)

    def send_threadsave(self, item: Any) -> NoReturn:
        for sp in self:
            sp.send_threadsave(item)
