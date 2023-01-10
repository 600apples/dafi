import asyncio
import warnings
from enum import IntEnum
from asyncio import PriorityQueue
from typing import Any, Optional, ClassVar, Dict, NoReturn

from daffi.interface import AbstractQueue


__all__ = ["ItemPriority", "FreezableQueue"]


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

    def __init__(self):
        self._queue = PriorityQueue()
        self._is_frozen = False  # Flag that indicated whether currently queue receiving is frozen.
        self.loop = asyncio.get_running_loop()
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
        while True:
            try:
                self._queue.get_nowait()
            except Exception:
                break
        self.reset()

    async def iterate(self):
        """Start handling tasks in the cycle."""

        while True:
            if not self._is_frozen:
                queue_entry = await self._queue.get()
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

    def send(self, data: Any, priority: Optional[ItemPriority] = ItemPriority.NORMAL):
        """
        Threadsave option to send item to queue.
        Put one item into queue.
        Item will be processed with NORMAL priority. This value cannot be changed.
        """
        self._queue.put_nowait(PriorityEntry(priority=priority, data=data))

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
        _queue = cls.queues.get(ident)
        if not _queue or _queue._closed:
            _queue = FreezableQueue()
            cls.queues[ident] = _queue
        return _queue

    @classmethod
    def factory_remove(cls, ident: str) -> NoReturn:
        cls.queues.pop(ident, None)

    @classmethod
    async def clear_all(cls) -> NoReturn:
        for q in cls.queues.values():
            await q.clear()
            await q.stop()
        cls.queues.clear()

    @classmethod
    async def wait_all(cls) -> NoReturn:
        for q in cls.queues.values():
            await q.wait()
