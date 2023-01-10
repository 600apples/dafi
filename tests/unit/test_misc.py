import pytest
from anyio import create_task_group, sleep
from daffi.utils.misc import Period, ConditionObserver, async_library
from daffi.exceptions import InitializationError


class TestMiscSuite:
    async def test_period_arguments(self):

        with pytest.raises(InitializationError):
            Period(at_time=None, interval=None).validate()

        with pytest.raises(InitializationError):
            Period(at_time=123, interval=45).validate()

        with pytest.raises(InitializationError):
            Period(at_time=list(range(11111111)), interval=None).validate()

        with pytest.raises(InitializationError):
            Period(at_time=list(range(5)), interval=None).validate()

        with pytest.raises(InitializationError):
            Period(interval=-1).validate()

    async def test_condition_observer_fail(self):
        res = ""

        def done_cb():
            nonlocal res
            res = True

        def fail_cb():
            nonlocal res
            res = False

        c = ConditionObserver(condition_timeout=3)
        c.register_done_callback(done_cb)
        c.register_fail_callback(fail_cb)

        await c.fire()
        assert res is False

    async def test_condition_observer_true(self):
        res = None

        async def done_cb():
            nonlocal res
            res = True

        async def fail_cb():
            nonlocal res
            res = False

        c = ConditionObserver(condition_timeout=3)
        c.register_done_callback(done_cb)
        c.register_fail_callback(fail_cb)

        async with create_task_group() as sg:
            sg.start_soon(c.fire)
            await sleep(1)
            await c.done()

        assert res is True


    async def test_async_library(self):
        assert async_library() == "asyncio"
