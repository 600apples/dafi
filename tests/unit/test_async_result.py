import asyncio
import pytest
from anyio import to_thread
from daffi.async_result import AsyncResult, SchedulerTask, get_result_type, RpcMessage, IterableAsyncResult
from daffi.exceptions import TimeoutError, RemoteError, RemoteCallError


async def set_result(wait: int, uuid, result):
    await asyncio.sleep(wait)
    AsyncResult._set_and_trigger(uuid, result)


msg = RpcMessage(func_name="abc", uuid=12345)


class TestAsyncResultSuite:
    async def test_timeout(self):

        ares = AsyncResult(msg=msg)._register()
        with pytest.raises(TimeoutError):
            ares.get(2)

    async def test_timeout_async(self):

        ares = AsyncResult(msg=msg)._register()
        with pytest.raises(TimeoutError):
            await ares.get_async(2)

    async def test_raise_remote_error(self):
        ares = AsyncResult(msg=msg)._register()
        AsyncResult._awaited_results[12345] = RemoteError(info="abc")

        with pytest.raises(RemoteCallError):
            await ares.get_async(2)

    async def test_get_result(self):
        ares = AsyncResult(msg=msg)._register()
        asyncio.create_task(set_result(1, 12345, "test"))
        res = await to_thread.run_sync(ares.get, 5)
        assert res == "test"
        # second time the same result
        assert ares.get() == "test"

    async def test_get_result_async(self):
        ares = AsyncResult(msg=msg)._register()
        asyncio.create_task(set_result(1, 12345, "test"))
        res = await ares.get_async(5)
        assert res == "test"
        # second time the same result
        assert ares.get() == "test"

    async def test_get_result_type(self):
        res = get_result_type(False, False)
        assert isinstance(res, type(AsyncResult))

        res = get_result_type(False, True)
        assert isinstance(res, type(SchedulerTask))

        res = get_result_type(True, True)
        assert isinstance(res, type(IterableAsyncResult))


class TestScheduledTaskSuite:
    async def test_get_result(self):
        ares = SchedulerTask(msg=msg)._register()
        asyncio.create_task(set_result(1, 12345, "test"))
        res = await to_thread.run_sync(ares.get)
        assert res._transmitter == "test"
        # second time the same result
        assert ares.get()._transmitter == "test"

    async def test_get_result_async(self):
        ares = SchedulerTask(msg=msg)._register()
        asyncio.create_task(set_result(1, 12345, "test"))
        res = await ares.get_async()
        assert res == ares
        assert res._transmitter == "test"
        # second time
        res = await ares.get_async()
        assert res == ares
        assert res._transmitter == "test"
