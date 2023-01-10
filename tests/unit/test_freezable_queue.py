import time
import asyncio
from daffi.components.operations.freezable_queue import FreezableQueue, ItemPriority, STOP_MARKER


class TestFreezebleQueue:
    async def test_queue_size(self):
        # Preparation
        fq = FreezableQueue()

        # Execution & Assertion
        assert fq.size == 0
        fq.send(1)
        assert fq.size == 1

        for _ in range(100):
            fq.send(1)

        assert fq.size == 101
        await fq._queue.get()
        assert fq.size == 100

        for _ in range(100):
            await fq._queue.get()

        assert fq.size == 0

    async def test_item_priority_last(self):
        """
        Test stop marker normally delivered as last item in queue.
        This way we allow all products to be processed before terminating workers
        """
        # Preparation
        fq = FreezableQueue()

        for _ in range(3):
            fq.send(1)

        await fq.stop(priority=ItemPriority.LAST)

        res = await fq._queue.get()
        assert res.data == 1
        res = await fq._queue.get()
        assert res.data == 1
        res = await fq._queue.get()
        assert res.data == 1

        res = await fq._queue.get()
        assert res.data == STOP_MARKER

    async def test_item_priority_first(self):
        """
        Test stop marker normally delivered as first item in queue.
        This way we force terminate workers without processing products
        """
        # Preparation
        fq = FreezableQueue()

        for _ in range(3):
            fq.send(1)

        await fq.stop(ItemPriority.FIRST)

        res = await fq._queue.get()
        assert res.data == STOP_MARKER

    async def test_task_done(self):
        # Preparation
        fq = FreezableQueue()

        for _ in range(3):
            fq.send(1)

        # Execution & Assertion
        assert fq.unfinished_tasks == 3
        assert fq.task_done() is True
        assert fq.unfinished_tasks == 2
        assert fq.task_done() is True
        assert fq.unfinished_tasks == 1
        assert fq.task_done() is True
        assert fq.unfinished_tasks == 0
        assert fq.task_done() is False
        assert fq.unfinished_tasks == 0

    async def test_reset_queue(self):
        # Preparation
        fq = FreezableQueue()

        for _ in range(100):
            fq.send(1)

        assert fq.unfinished_tasks == 100
        fq.reset()
        assert fq.unfinished_tasks == 0

    async def test_iterate(self):
        # Preparation
        fq = FreezableQueue()

        for _ in range(100):
            fq.send(1)

        # Normally it just send stop marker which allow cycle to be terminated after all items are processed
        await fq.stop(priority=ItemPriority.LAST)

        # Execution & Assertion
        task_done_counter = 101
        async for item in fq.iterate():

            assert item == 1
            assert fq.unfinished_tasks == task_done_counter
            task_done_counter -= 1

        assert fq.unfinished_tasks == 0
        assert fq.size == 0

    async def test_queue_freeze(self):
        """
        Test after sending 'freeze' signal queue gets frozen for specified time.
        No items can be taken from queue during this time.
        """
        # Preparation
        fq = FreezableQueue()
        fq.send(1)

        # Normally it just send stop marker which allow cycle to be terminated after all items are processed
        await fq.stop(priority=ItemPriority.LAST)

        initial_time = time.time()
        fq.freeze(10)  # freeze for 10 seconds

        async for _ in fq.iterate():
            await asyncio.sleep(0.1)  # Simulate work

        delta = time.time() - initial_time

        # Assertion
        # Check queue was frozen approximately 10 sec.
        assert int(delta) == 10
