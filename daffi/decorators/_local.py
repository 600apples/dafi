from typing import Callable, Any


def local(fn: Callable[..., Any] = None):
    """
    `local` decorator is used to skip callback or fetcher initialization for function.
    IOW any method decorated with `local` is just regular local method but not fetcher or callback.
    """

    def dec(fn: Callable[..., Any]):
        if hasattr(fn, "local") or "local" not in fn.__dict__:
            fn.local = True
        return fn

    if callable(fn):
        return dec(fn)

    else:
        return dec
