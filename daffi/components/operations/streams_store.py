import time
import asyncio
from dataclasses import dataclass, field
from contextlib import contextmanager
from typing import Any, NoReturn, List, Union, Tuple
from grpc.aio._call import AioRpcError
from daffi.components.operations.freezable_queue import FreezableQueue, QueueMixin


@dataclass
class StreamPair(QueueMixin):
    """
    StreamPair represents message iterator to remote process.
    'Pair' because it should be unique instance for transmitter/receiver pair.
    """

    q: FreezableQueue
    _closed: bool = field(repr=False, default=False)

    def __post_init__(self):
        super().__init__()

    # create an instance of the iterator
    async def __aiter__(self):
        try:
            async for msg in self.q.iterate():
                yield msg
        except AioRpcError as err:
            if err.details != "Cancelling all calls":
                raise

    @property
    def closed(self) -> bool:
        """Return True if current StreamPair is closed."""
        return self._closed

    @closed.setter
    def closed(self, new_val) -> NoReturn:
        """Mark current StreamPair as closed."""
        self._closed = new_val


class StreamPairStore(dict):
    def __init__(self):
        super().__init__()
        self.loop = asyncio.get_running_loop()
        self.stream_pairs_queue = asyncio.Queue()
        self.stream_pair_group_store = dict()

    def create_stream_pair(self, *strings) -> StreamPair:
        key = "-".join(strings)
        stream_pair = StreamPair(q=FreezableQueue(self.loop))
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
    def get_or_create_stream_pair_cm(self, *strings) -> StreamPair:
        stream_pair = self.get_or_create_stream_pair(*strings)
        try:
            yield stream_pair
        finally:
            self.delete_stream_pair(*strings)

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
            self.stream_pair_group_store[msg_uuid] = stream_pair_group
            yield stream_pair_group
        finally:
            for stream_pair in stream_pair_group:
                stream_pair.stop_threadsave()
            for receiver in receivers:
                self.delete_stream_pair(receiver, msg_uuid)
            self.stream_pair_group_store.pop(msg_uuid, None)

    async def accept_multi_connection(self) -> Tuple["StreamPairGroup", str]:
        return await self.stream_pairs_queue.get()


class StreamPairGroup(List[StreamPair]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.throttle_time = 0

    @property
    def closed(self) -> bool:
        return any(sp.closed for sp in self)

    def send_threadsave(self, item: Any) -> NoReturn:
        if self.throttle_time:
            time.sleep(self.throttle_time)
        for sp in self:
            sp.send_threadsave(item)
