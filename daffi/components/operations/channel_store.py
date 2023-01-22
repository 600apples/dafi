import logging
import asyncio
import traceback
from typing import Optional, Union, Generator, NoReturn

from grpc._cython.cygrpc import UsageError

from daffi.components.proto.message import Message, RpcMessage, ServiceMessage
from daffi.utils.misc import ConditionObserver
from daffi.utils.settings import RECONNECTION_TIMEOUT
from daffi.components.operations.freezable_queue import FreezableQueue, QueueMixin
from daffi.async_result import AsyncResult, RemoteError


logger = logging.getLogger(__name__)


class MessageIterator(QueueMixin):
    """Iterator to recieve messages"""

    def __init__(self, msg_queue: FreezableQueue):
        self.q = msg_queue

    # create an instance of the iterator
    async def __aiter__(self):
        async for message, eta in self.q.iterate():

            if eta:
                # Put to queue again after eta.
                # It prevents other message to be pushed meanwhile.
                asyncio.create_task(self.send_with_eta(message=message, eta=eta))
            else:
                try:
                    for chunk in message.dumps():
                        yield chunk
                except Exception as e:
                    if AsyncResult._awaited_results.get(message.uuid):
                        AsyncResult._set_and_trigger(
                            message.uuid, RemoteError(info=str(e), _origin_traceback=e.__traceback__)
                        )
                    else:
                        traceback.print_exc()

    def send_threadsave(self, message: Message, eta: Optional[Union[int, float]] = None) -> NoReturn:
        self.q.send_threadsave((message, eta))

    async def send(self, message: Message, eta: Optional[Union[int, float]] = None) -> NoReturn:
        await self.q.send((message, eta))

    async def send_with_eta(self, message: Message, eta: Union[int, float]) -> NoReturn:
        await asyncio.sleep(eta)
        await self.send(message)


class ChannelPipe(ConditionObserver):
    def __init__(
        self,
        send_iterator: MessageIterator,
        receive_iterator: Generator,
        reconnection_timeout: Optional[int] = RECONNECTION_TIMEOUT,
    ):
        self.send_iterator = send_iterator
        self.receive_iterator = receive_iterator
        super().__init__(condition_timeout=reconnection_timeout)

    # create an instance of the iterator
    async def __aiter__(self) -> Generator[Union[RpcMessage, ServiceMessage], None, None]:
        try:
            async for msg in Message.from_message_iterator(self.receive_iterator):
                yield msg
        except UsageError:
            ...

    def send_threadsave(self, message: Message, eta: Optional[Union[int, float]] = None):
        self.send_iterator.send_threadsave(message, eta)

    async def send(self, message: Message, eta: Optional[Union[int, float]] = None):
        await self.send_iterator.send(message, eta)

    def freeze(self, timeout: Optional[int] = RECONNECTION_TIMEOUT) -> NoReturn:
        self.send_iterator.freeze(timeout=timeout)

    def proceed(self) -> NoReturn:
        self.send_iterator.proceed()

    async def clear_queue(self):
        await self.send_iterator.q.clear()
        await self.send_iterator.q.wait()

    async def stop(self):
        await self.send_iterator.stop()


class ChannelStore(dict):
    async def add_channel(self, channel: ChannelPipe, process_name: str) -> bool:
        was_locked = False
        prev_chan = self.get(process_name)
        if prev_chan and prev_chan.locked:
            was_locked = True
            await prev_chan.done()
        self[process_name] = channel
        return was_locked

    async def find_process_name_by_channel(self, channel: ChannelPipe) -> str:
        try:
            return next(proc for proc, chan in list(self.items()) if chan == channel)
        except StopIteration:
            ...

    async def delete_channel(self, process_name: str):
        self.pop(process_name, None)

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
