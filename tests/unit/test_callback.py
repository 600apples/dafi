import pytest
import asyncio
from dafi import callback, Global
from dafi.utils.settings import LOCAL_CALLBACK_MAPPING
from dafi.globals import RemoteCallback
from dafi.exceptions import InitializationError, GlobalContextError


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
        assert isinstance(add._fn, RemoteCallback)
        assert isinstance(substract._fn, RemoteCallback)

    async def test_local_callback_execution_with_g(self):
        Global._instances.clear()

        @callback
        def func_with_g(g: Global) -> Global:
            return g

        with pytest.raises(KeyError):
            func_with_g()

        with Global(init_node=False, init_controller=True, process_name="foo"):
            g = func_with_g()

        assert g.process_name == "foo"
        assert g.init_controller == True
        assert g.init_node == False
        assert g.host is None
        assert g.port is None

    async def test_local_class_callback_execution(self):
        Global._instances.clear()

        @callback
        class Foo:
            def func1(self):
                return "func1"

            def func2(self, a, b):
                return a + b

            def func3(self, g):
                return g

            @classmethod
            def func4(cls):
                return "classmethod"

            @staticmethod
            def func5():
                return "staticmethod"

            def func6(self, *args, g):
                return None

        with Global(init_controller=True) as g:
            node_callback_mapping = g.ipc.node.node_callback_mapping
            controller_callback_mapping = g.ipc.controller.controller_callback_mapping

            await asyncio.sleep(1)
            node_process_mapping = next(v for k, v in node_callback_mapping.items())
            controller_process_mapping = next(v for k, v in controller_callback_mapping.items())

            assert set(node_process_mapping) == {"func1", "func2", "func3", "func4", "func5", "func6"}
            assert set(controller_process_mapping) == {"func1", "func2", "func3", "func4", "func5", "func6"}

            foo = Foo()

            assert foo.func1() == "func1"
            assert foo.func2(1, 2) == 3
            assert isinstance(foo.func3(), Global)
            assert foo.func4() == "classmethod"
            assert foo.func5() == "staticmethod"

            with pytest.raises(InitializationError):
                Foo()

            validate_provided_arguments = foo.func6.validate_provided_arguments

            validate_provided_arguments()
            validate_provided_arguments(1,2,3,4,5)

            with pytest.raises(GlobalContextError):
                validate_provided_arguments(g)

            with pytest.raises(GlobalContextError):
                validate_provided_arguments(g=None)

            with pytest.raises(TypeError):
                validate_provided_arguments(foo='bar')