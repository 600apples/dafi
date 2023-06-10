from daffi import Global
from daffi.decorators import fetcher, alias
from daffi.settings import LOCAL_FETCHER_MAPPING


class TestFecherSuite:
    async def test_callback_alias(self):
        Global._instances.clear()

        @fetcher
        @alias("custom1")
        def my_func1():
            pass

        @alias("custom2")
        @fetcher
        def my_func2():
            pass

        @fetcher
        def my_func3():
            pass

        assert my_func1.alias == "custom1"
        assert my_func2.alias == "custom2"
        assert my_func3.alias == "my_func3"
        assert f"{id(my_func1.wrapped)}-custom1" in LOCAL_FETCHER_MAPPING
        assert f"{id(my_func2.wrapped)}-custom2" in LOCAL_FETCHER_MAPPING
        assert f"{id(my_func3.wrapped)}-my_func3" in LOCAL_FETCHER_MAPPING
