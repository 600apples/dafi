import pytest
from anyio import create_task_group, sleep
from daffi.utils.misc import Period, async_library
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

    async def test_async_library(self):
        assert async_library() == "asyncio"
