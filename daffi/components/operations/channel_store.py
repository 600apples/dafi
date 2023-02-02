import logging
import traceback
from typing import Optional, Union, Generator, NoReturn

from grpc._cython.cygrpc import UsageError

from daffi.utils.misc import call_after
from daffi.components.proto.message import Message, RpcMessage, ServiceMessage
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
                await call_after(eta, self.send, message=message)
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


class ChannelPipe:
    def __init__(
        self,
        send_iterator: MessageIterator,
        receive_iterator: Generator,
    ):
        self.send_iterator = send_iterator
        self.receive_iterator = receive_iterator

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

    def freeze(self, timeout: Optional[int] = 15) -> NoReturn:
        self.send_iterator.freeze(timeout=timeout)

    def proceed(self) -> NoReturn:
        self.send_iterator.proceed()

    async def clear_queue(self):
        await self.send_iterator.q.clear()
        await self.send_iterator.q.wait()

    async def stop(self):
        await self.send_iterator.stop()


class ChannelStore(dict):
    async def add_channel(self, channel: ChannelPipe, process_name: str):
        self[process_name] = channel

    async def find_process_name_by_channel(self, channel: ChannelPipe) -> str:
        try:
            return next(proc for proc, chan in list(self.items()) if chan == channel)
        except StopIteration:
            ...

    async def delete_channel(self, process_name: str):
        self.pop(process_name, None)

    async def iterate(self):
        for proc_name in list(self):
            if chan := await self.get_chan(proc_name):
                yield proc_name, chan

    async def get_chan(self, process_name: str) -> Optional[ChannelPipe]:
        return super().get(process_name)
