import asyncio
from itertools import chain
from contextlib import contextmanager
from functools import wraps
from typing import Optional, Union, Callable, Any, NamedTuple, Type, Sequence

from anyio import sleep, Event as asyncEvent
from threading import Event as thEvent

__all__ = ["resilent", "stoppable_retry"]

acceptable_errors = Optional[Union[Type[BaseException], Sequence[Type[BaseException]]]]


@contextmanager
def resilent(acceptable: acceptable_errors = Exception):
    """Suppress exceptions raised from the wrapped scope."""
    try:
        yield
    except acceptable:
        ...


class RetryInfo(NamedTuple):
    attempt: int
    prev_error: Optional[type(Exception)] = None


class AsyncRetry:
    def __init__(
        self, stop_event: Union[thEvent, asyncEvent], fn: Callable[..., Any], acceptable: acceptable_errors, wait: int
    ):
        self.fn = fn
        self.stop_event = stop_event
        self.acceptable = acceptable
        self.wait = wait

    async def stop_event_observer(self) -> bool:
        while True:
            await sleep(0.1)
            if self.stop_event.is_set():
                return True

    async def __call__(self, *args, **kwargs):
        stop_event_observer = "stop_event_observer"
        attempt = 0
        prev_error = None

        while True:
            attempt += 1
            kwargs["retry_info"] = RetryInfo(attempt=attempt, prev_error=prev_error)

            is_stop = asyncio.create_task(self.stop_event_observer(), name=stop_event_observer)
            done, pending = chain.from_iterable(
                await asyncio.wait([self.fn(*args, **kwargs), is_stop], return_when=asyncio.FIRST_COMPLETED)
            )

            pending.cancel()
            # Take first element from set. We know there is always 1 task in done and 1 in pending state.
            if done.get_name() == stop_event_observer:
                break

            try:
                return done.result()
            except self.acceptable as err:
                prev_error = err
                await sleep(self.wait)


def stoppable_retry(wait: int, acceptable: acceptable_errors = Exception):
    def dec(fn: Callable[..., Any]) -> Any:
        @wraps(fn)
        async def _dec(*args, **kwargs) -> Any:
            try:
                stop_event = next(filter(lambda i: isinstance(i, (asyncEvent, thEvent)), args))
            except StopIteration:
                raise KeyError(
                    f"Unable to find stop event argument. Please provide {type(thEvent)} or {type(asyncEvent)}"
                )
            return await AsyncRetry(fn=fn, stop_event=stop_event, acceptable=acceptable, wait=wait)(*args, **kwargs)

        return _dec

    return dec
