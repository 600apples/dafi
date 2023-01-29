import time
import asyncio
import warnings
from enum import IntEnum
from asyncio import PriorityQueue
from contextlib import asynccontextmanager
from typing import Any, Optional, ClassVar, Dict, NoReturn

from daffi.interface import AbstractQueue


__all__ = ["ItemPriority", "FreezableQueue", "QueueMixin"]


class ItemPriority(IntEnum):
    """Determine data priority for queue processing"""

    FIRST = 1
    NORMAL = 2
    LAST = 3


class PriorityEntry:
    def __init__(self, priority: ItemPriority, data: Any):
        self.data = data
        self.priority = priority

    def __lt__(self, other: "PriorityEntry"):
        return self.priority < other.priority


# Internal marker which indicate that receiving new items must be interrupted.
STOP_MARKER = object()


class FreezableQueue(AbstractQueue):
    """
    Extended asyncio.PriorityQueue with additional option to freeze receiving new items for provided period of time.
    """

    queues: ClassVar[Dict[str, Any]] = dict()

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.loop = loop or asyncio.get_running_loop()
        if loop:
            try:
                existing_loop = asyncio.get_running_loop()
            except RuntimeError:
                existing_loop = loop
            asyncio.set_event_loop(existing_loop)

        self._queue = PriorityQueue()
        self._queue._loop = self.loop
        self._is_frozen = False  # Flag that indicated whether currently queue receiving is frozen.
        self._closed = False

    @property
    def size(self) -> int:
        """Number of items in queue"""
        return self._queue.qsize()

    @property
    def unfinished_tasks(self) -> int:
        """Get count of unfinished tasks from the internal queue counter"""
        return self._queue._unfinished_tasks

    def freeze(self, timeout: int) -> NoReturn:
        """
        Freeze 'iterate' for provided timeout.
        Args:
            timeout: Period of time in seconds for which 'iterate' generator will be suspended.
        """
        self._is_frozen = True
        try:
            self.t.cancel()
        except AttributeError:
            ...
        self.t = self.loop.call_later(timeout, callback=lambda: setattr(self, "_is_frozen", False))

    def proceed(self) -> NoReturn:
        """Deactivate _is_frozen flag in order to continue serving"""
        self._is_frozen = False

    def task_done(self) -> bool:
        """Notify task done"""
        try:
            self._queue.task_done()
            return True
        except ValueError:
            return False

    def reset(self) -> None:
        """Reset internal tasks counter in order to enforce pending processes to continue working"""
        while True:
            if not self.task_done():
                break

    async def clear(self):
        """Clear queue."""
        while True:
            try:
                self._queue.get_nowait()
            except Exception:
                break
        self.reset()

    async def get(self):
        """Get one item from queue in current thread."""
        return await self._queue.get()

    async def iterate(self):
        """Start handling tasks in the cycle."""

        while True:
            if not self._is_frozen:
                try:
                    queue_entry = await self._queue.get()
                except asyncio.exceptions.CancelledError:
                    break
                data = queue_entry.data

                if data == STOP_MARKER:
                    # Exit from iterator
                    self.task_done()
                    return

                try:
                    yield data
                finally:
                    # Mark task as ready. It is important to do that after each action as
                    # queue keeps internal tasks counter
                    self.task_done()

            else:
                # If queue is frozen due to quota rate limit exceeded or other related issue we need to wait a bit.
                await asyncio.sleep(0.5)

    def send_threadsave(self, data: Any, priority: Optional[ItemPriority] = ItemPriority.NORMAL):
        """
        Threadsave option to send item to queue.
        Put one item into queue.
        Item will be processed with NORMAL priority. This value cannot be changed.
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                asyncio.run_coroutine_threadsafe(
                    self._queue.put(PriorityEntry(priority=priority, data=data)), self.loop
                ).result()
            except RuntimeError:
                ...

    def send_no_wait(self, data: Any, priority: Optional[ItemPriority] = ItemPriority.NORMAL):
        self._queue.put_nowait(PriorityEntry(priority=priority, data=data))

    async def send(self, data: Any, priority: Optional[ItemPriority] = ItemPriority.NORMAL):
        """
        Threadsave option to send item to queue.
        Put one item into queue.
        Item will be processed with NORMAL priority. This value cannot be changed.
        """
        await self._queue.put(PriorityEntry(priority=priority, data=data))

    async def send_with_time_priority(self, data: Any):
        """Send item to queue with priority based on current timestamp"""
        await self._queue.put(PriorityEntry(priority=time.time(), data=data))

    async def wait(self) -> None:
        """Wait until queue is empty and all tasks finished."""
        await self._queue.join()

    async def stop(self, priority: ItemPriority = ItemPriority.LAST):
        """Stop iterator"""
        if not self._closed:
            self._closed = True
            await self._queue.put(PriorityEntry(priority=priority, data=STOP_MARKER))

    @classmethod
    def factory(cls, ident: str) -> "FreezableQueue":
        """
        Create new FreezableQueue by given 'ident' key
        or return existing queue if such key already exist in 'queues' dictionary
        """
        _queue = cls.queues.get(ident)
        if not _queue or _queue._closed:
            _queue = FreezableQueue()
            cls.queues[ident] = _queue
        return _queue

    @classmethod
    def factory_remove(cls, ident: str) -> NoReturn:
        """Remove queue from 'queues' dict by givent 'ident' key"""
        cls.queues.pop(ident, None)

    @classmethod
    async def clear_all(cls) -> NoReturn:
        """Clear all queues in 'queues' dictionary."""
        for q in cls.queues.values():
            await q.clear()
            await q.stop()
        cls.queues.clear()

    @classmethod
    async def wait_all(cls) -> NoReturn:
        """Wait all queues in queues dict to process all items. Items should be processed via 'iterate' method."""
        for q in cls.queues.values():
            await q.wait()


class QueueMixin:
    """
    Enrich parent class with method related to working with FreezableQueue.
    Assumed that parent class has attribute 'q' which is FreezableQueue.
    """

    async def get(self) -> Any:
        """Get one item from queue"""
        return await self.q.get()

    async def wait(self) -> NoReturn:
        """Wait all queue jobs marked as done"""
        await self.q.wait()

    def task_done(self) -> bool:
        """Mark job as done"""
        return self.q.task_done()

    def send_threadsave(self, item: Any, priority: Optional[ItemPriority] = ItemPriority.NORMAL) -> NoReturn:
        """Send item with given priority from different thread"""
        self.q.send_threadsave(item, priority)

    def send_no_wait(self, item):
        """Send item from current thread but not wait it to be taken"""
        self.q.send_no_wait(item)

    async def send(self, item: Any, priority: Optional[ItemPriority] = ItemPriority.NORMAL) -> NoReturn:
        """Send item from current thread and wait it to be taken"""
        await self.q.send(item, priority=priority)

    async def send_with_time_priority(self, item: Any):
        await self.q.send_with_time_priority(item)

    def stop_threadsave(self, priority: Optional[ItemPriority] = ItemPriority.LAST) -> NoReturn:
        """Send stop marker to queue from different thread"""
        self.send_threadsave(STOP_MARKER, priority)

    async def stop(self, priority: Optional[ItemPriority] = ItemPriority.LAST) -> NoReturn:
        """Send stop marker to queue from current thread"""
        await self.q.stop(priority)

    def freeze(self, timeout: int) -> NoReturn:
        """Freeze queue during given timeout."""
        self.q.freeze(timeout=timeout)

    def proceed(self) -> NoReturn:
        """Unfreeze queue"""
        self.q.proceed()

    async def transmit_item(self, item: Any):
        """
        This method works in pair with 'accept_item'
            1. method1 send any item via queue
            2. method1 waits until method2 take item from queue
            3. method2 marks job done. method1 starts waiting stop marker received via the same queue
        """
        await self.send(item=item)
        await self.wait()
        await self.get()

    @asynccontextmanager
    async def accept_item(self):
        """
        This method works in pair with 'transmit_item'
            1. method2 accept item from queue and mark task as done
            2. after any necessary actions with transmitted object method2 sends stop marker to queue.
        """
        data = await self.get()
        self.task_done()
        try:
            yield data.data
        finally:
            await self.send(item=STOP_MARKER)
