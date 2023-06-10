from daffi.registry import Callback, Fetcher
from daffi.registry._fetcher import LOCAL_FETCHER_MAPPING
from daffi.registry._callback import LOCAL_CALLBACK_MAPPING
from daffi.decorators import local, alias, __body_unknown__


class TestCallbackAndFetcherSuite:
    def test_stores_callback(self):
        class RegistryGroup(Callback):
            auto_init = True

            def method1(self):
                return "method1"

            def method2(self):
                return "method2"

            @alias("method1")
            def method3(self):
                return "method3"

            @local
            def method4(self):
                return "method4"

        assert "method1" in LOCAL_CALLBACK_MAPPING
        assert "method2" in LOCAL_CALLBACK_MAPPING
        assert "method3" not in LOCAL_CALLBACK_MAPPING
        assert "method4" not in LOCAL_CALLBACK_MAPPING

    def test_stores_fetcher(self):
        class RegistryGroup(Fetcher):
            def method1(self):
                pass

            def method2(self):
                __body_unknown__()

            @alias("method1")
            def method3(self):
                return "method3"

            @local
            def method4(self):
                return "method4"

        rg = RegistryGroup()
        rg_id = id(rg)

        assert f"{rg_id}-method1" in LOCAL_FETCHER_MAPPING
        assert f"{rg_id}-method2" in LOCAL_FETCHER_MAPPING
        assert f"{rg_id}-method3" in LOCAL_FETCHER_MAPPING

        assert LOCAL_FETCHER_MAPPING[f"{rg_id}-method1"].proxy_ == True
        assert LOCAL_FETCHER_MAPPING[f"{rg_id}-method2"].proxy_ == True
        assert LOCAL_FETCHER_MAPPING[f"{rg_id}-method3"].proxy_ == False

        assert LOCAL_FETCHER_MAPPING[f"{rg_id}-method1"].origin_method.__name__ == "method1"
        assert LOCAL_FETCHER_MAPPING[f"{rg_id}-method1"].origin_name_ == "method1"

        assert LOCAL_FETCHER_MAPPING[f"{rg_id}-method2"].origin_method.__name__ == "method2"
        assert LOCAL_FETCHER_MAPPING[f"{rg_id}-method2"].origin_name_ == "method2"

        assert LOCAL_FETCHER_MAPPING[f"{rg_id}-method3"].origin_method.__name__ == "method3"
        assert LOCAL_FETCHER_MAPPING[f"{rg_id}-method3"].origin_name_ == "method1"

        assert f"{rg_id}-method4" not in LOCAL_FETCHER_MAPPING
