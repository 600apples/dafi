import pytest
from daffi import Global, BROADCAST, FG
from daffi.decorators import callback, fetcher, alias
from daffi.settings import LOCAL_CALLBACK_MAPPING, LOCAL_FETCHER_MAPPING
from daffi.method_executors import CallbackExecutor
from daffi.exceptions import InitializationError


class TestCallbackSuite:
    async def test_local_callback_execution(self):
        Global._instances.clear()

        @callback
        def add(a: int, b: int) -> int:
            return a + b

        @callback
        def substract(a: int, b: int) -> int:
            return a - b

        # Assertion
        assert add(1, 2) == 3
        assert substract(10, 5) == 5
        assert isinstance(add, callback)
        assert isinstance(substract, callback)
        assert "add" in LOCAL_CALLBACK_MAPPING
        assert "substract" in LOCAL_CALLBACK_MAPPING
        assert isinstance(add._fn, CallbackExecutor)
        assert isinstance(substract._fn, CallbackExecutor)

        with Global(init_controller=True) as g:
            with pytest.raises(TypeError):
                substract(1, 2, 4)

            with pytest.raises(TypeError):
                substract(1)

            with pytest.raises(TypeError):
                substract(1, 2, 4)

    async def test_local_class_callback_execution(self):
        Global._instances.clear()

        with pytest.raises(InitializationError):

            @callback
            class Foo:
                def func1(self):
                    return "func1"

                def func2(self, a, b):
                    return a + b

    async def test_callback_and_fetcher_over_one_function(self):
        Global._instances.clear()

        @callback
        @fetcher(exec_modifier=BROADCAST)
        def my_func1():
            pass

        assert "my_func1" in LOCAL_CALLBACK_MAPPING
        assert f"{id(my_func1.wrapped)}-my_func1" in LOCAL_FETCHER_MAPPING
        assert isinstance(my_func1, fetcher)
        assert my_func1.proxy is True
        assert my_func1.exec_modifier == BROADCAST
        my_func1.exec_modifier = FG
        assert my_func1.exec_modifier == FG

        @fetcher
        @callback
        def my_func2():
            pass

        assert "my_func2" in LOCAL_CALLBACK_MAPPING
        assert f"{id(my_func2.wrapped)}-my_func2" in LOCAL_FETCHER_MAPPING
        assert isinstance(my_func2, fetcher)
        assert my_func2.proxy is True
        assert my_func2.exec_modifier == FG
        my_func2.exec_modifier = BROADCAST
        assert my_func2.exec_modifier == BROADCAST

    async def test_callback_alias(self):
        Global._instances.clear()

        @callback
        @alias("custom1")
        def my_func1():
            pass

        @alias("custom2")
        @callback
        def my_func2():
            pass

        assert my_func1.alias == "custom1"
        assert my_func2.alias == "custom2"
        assert "custom1" in LOCAL_CALLBACK_MAPPING
        assert "custom2" in LOCAL_CALLBACK_MAPPING
