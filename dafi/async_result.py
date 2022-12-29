import time
from anyio import sleep
from dataclasses import dataclass
from threading import Event
from typing import ClassVar, Dict, Optional, Union

from dafi.exceptions import RemoteError, TimeoutError, RemoteStoppedUnexpectedly
from dafi.utils.custom_types import RemoteResult


@dataclass
class AsyncResult:
    _awaited_results: ClassVar[Dict[str, "AsyncResult"]] = dict()

    func_name: str
    uuid: str
    result: Optional[RemoteResult] = None

    def __post_init__(self):
        self._ready = Event()

    def __call__(self, timeout: Union[int, float] = None) -> RemoteResult:
        if self.result is None:
            self._ready.wait(timeout=timeout)
            self.result = self._awaited_results.pop(self.uuid)

            if isinstance(self.result, RemoteError):
                self.result.raise_with_trackeback()
                import pdb

                pdb.set_trace()

            if self.result == self:
                self.result = None
                raise TimeoutError(f"Function {self.func_name} result timed out")

        return self.result

    def get(self, timeout: Union[int, float] = None) -> RemoteResult:
        return self(timeout=timeout)

    @classmethod
    def fold_results(cls):
        for msg_uuid, ares in AsyncResult._awaited_results.items():
            AsyncResult._awaited_results[msg_uuid] = RemoteError(
                info="Lost connection to Controller.",
                _awaited_error_type=RemoteStoppedUnexpectedly,
            )
            ares._ready.set()


class AwaitableAsyncResult(AsyncResult):
    async def __call__(self, timeout: Union[int, float] = None) -> RemoteResult:
        if self.result is None:
            if timeout is not None:
                timeout = time.time() + timeout

                def timeout_cond():
                    return time.time() < timeout

            else:

                def timeout_cond():
                    return True

            while timeout_cond() and not self._ready.is_set():
                await sleep(0.1)

            self.result = self._awaited_results.pop(self.uuid)

            if isinstance(self.result, RemoteError):
                self.result.raise_with_trackeback()
                import pdb

                pdb.set_trace()

            if self.result == self:
                self.result = None
                raise TimeoutError(f"Function {self.func_name} result timed out")

        return self.result

    async def get(self, timeout: Union[int, float] = None) -> RemoteResult:
        return await self(timeout=timeout)
