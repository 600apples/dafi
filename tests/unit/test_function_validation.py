import pytest
from daffi.decorators import alias
from daffi.exceptions import InitializationError
from daffi.utils.func_validation import func_info, is_class_or_static_method


class TestFunctionvalidationSuite:
    async def test_func_info(self):
        def func1():
            pass

        _, name = func_info(func1)
        assert name == "func1"

        @alias("my_func")
        def func2():
            pass

        _, name = func_info(func2)
        assert name == "my_func"

    async def test_is_class_or_static_method(self):
        class T:
            @staticmethod
            def func1():
                pass

            @classmethod
            def func2(self):
                pass

            def func3(self):
                pass

            @alias("abc")
            def func4(self):
                pass

        res = is_class_or_static_method(T, "func1")
        assert res == "static"

        res = is_class_or_static_method(T, "func2")
        assert res == "class"

        res = is_class_or_static_method(T, "func3")
        assert res is None

        res = is_class_or_static_method(T, "func4")
        assert res is None
