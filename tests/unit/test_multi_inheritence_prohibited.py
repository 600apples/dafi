import pytest
from daffi.registry import Fetcher, Callback
from daffi.exceptions import InitializationError


def test_multi_inheritence_prohibited():
    """Test class can be inherited only from one registry entity (Fetcher or Callback)"""
    with pytest.raises(InitializationError) as exc:

        class T(Fetcher, Callback):
            def my_func(self):
                pass

    assert "Multiple inheritance is prohibited" in exc.value.message

    with pytest.raises(InitializationError) as exc:

        class R(Callback, Fetcher):
            def my_func(self):
                pass

    assert "Multiple inheritance is prohibited" in exc.value.message

    with pytest.raises(TypeError) as exc:

        class P(Callback, Callback):
            def my_func(self):
                pass

    assert "duplicate base class Callbac" in exc.value.args[0]
