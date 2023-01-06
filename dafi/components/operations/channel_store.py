import logging
import asyncio
from asyncio import Queue
from typing import Optional, Union, Generator
from anyio import Event, sleep

from grpc.aio._call import AioRpcError
from grpc._cython.cygrpc import UsageError

from dafi.components.proto.message import Message, RpcMessage, ServiceMessage
from dafi.utils.debug import with_debug_trace
from dafi.utils.misc import ConditionObserver


logger = logging.getLogger(__name__)


class MessageIterator:

    STOP_MARKER = object()

    def __init__(self, global_terminate_event: Event):

        self.global_terminate_event = global_terminate_event
        self.msg_queue = Queue()
        self.loop = asyncio.get_running_loop()

    # create an instance of the iterator
    async def __aiter__(self):
        while not self.global_terminate_event.is_set():
            message, eta = await self.msg_queue.get()

            if message == self.STOP_MARKER:
                raise StopAsyncIteration

            if eta:
                await sleep(eta)

            for chunk in message.dumps():
                yield chunk

    def send_threadsave(self, message: Message, eta: Optional[Union[int, float]] = None):
        asyncio.run_coroutine_threadsafe(self.msg_queue.put((message, eta)), self.loop).result()

    def send(self, message: Message, eta: Optional[Union[int, float]] = None):
        self.msg_queue.put_nowait((message, eta))

    def stop(self):
        self.send(self.STOP_MARKER)


class ChannelPipe(ConditionObserver):
    def __init__(self, send_iterator: MessageIterator, receive_iterator: Generator):
        self.send_iterator = send_iterator
        self.receive_iterator = receive_iterator
        super().__init__(condition_timeout=15)

    # create an instance of the iterator
    async def __aiter__(self) -> Generator[Union[RpcMessage, ServiceMessage], None, None]:
        try:
            async for msg in Message.from_message_iterator(self.receive_iterator):
                yield msg
        except AioRpcError:
            self.send_iterator.stop()
            raise
        except UsageError:
            ...

    def send_threadsave(self, message: Message, eta: Optional[Union[int, float]] = None):
        self.send_iterator.send_threadsave(message, eta)

    def send(self, message: Message, eta: Optional[Union[int, float]] = None):
        self.send_iterator.send(message, eta)


class ChannelStore(dict):
    @with_debug_trace
    async def add_channel(self, channel: ChannelPipe, process_name: str) -> bool:
        was_locked = False
        prev_chan = self.get(process_name)
        if prev_chan and prev_chan.locked:
            was_locked = True
            await prev_chan.done()
        self[process_name] = channel
        return was_locked

    @with_debug_trace
    async def find_process_name_by_channel(self, channel: ChannelPipe) -> str:
        try:
            return next(proc for proc, chan in list(self.items()) if chan == channel)
        except StopIteration:
            ...

    @with_debug_trace
    async def delete_channel(self, process_name: str):
        self.pop(process_name, None)

    @with_debug_trace
    async def iterate(self):
        for proc_name in list(self):
            chan = await self.get_chan(proc_name)
            if chan:
                yield proc_name, chan

    async def get_chan(self, process_name: str) -> Optional[ChannelPipe]:
        while True:
            chan = super().get(process_name)
            if not chan:
                return
            elif chan.locked:
                await chan.wait()
            else:
                return chan
