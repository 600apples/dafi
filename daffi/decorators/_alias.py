from typing import Callable, Any


def alias(value: str):
    """Decorator for aliasing remote executors with the given value."""

    def dec(fn: Callable[..., Any]):

        if hasattr(fn, "alias") or "alias" not in fn.__dict__:
            fn.alias = value
        return fn

    return dec
