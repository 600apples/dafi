import pytest
from daffi.utils.misc import Period, async_library, contains_explicit_return
from daffi.exceptions import InitializationError


def deco(return_val):
    def __wrap(fn):
        return fn

    return __wrap


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

    def test_function_has_return_statement(self):
        def func1():
            return 1

        def func2():
            pass

        def func3():
            """
            return something
            return None
            """

        def func4():
            """No return"""
            # This function donesn't contain return
            # return

        def func5():
            """No return"""
            # This function donesn't contain return
            # return

        def func6():
            def dec():
                return None

        def func7(return_val1, return_val2=1):
            pass

        @deco(return_val=1)
        def func8():
            pass

        def return_val():
            pass

        def return_val2():
            """No return"""
            pass

        assert contains_explicit_return(func1) is True
        assert contains_explicit_return(func2) is False
        assert contains_explicit_return(func3) is False
        assert contains_explicit_return(func4) is False
        assert contains_explicit_return(func5) is False
        assert contains_explicit_return(func6) is True
        assert contains_explicit_return(func7) is False
        assert contains_explicit_return(func8) is False
        assert contains_explicit_return(return_val) is False
        assert contains_explicit_return(return_val2) is False
