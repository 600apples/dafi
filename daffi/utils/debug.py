import os
import logging
import traceback
from pathlib import Path


from anyio import EndOfStream
from grpc.aio._call import AioRpcError
from tenacity import retry, stop_after_attempt, retry_if_not_exception_type

from daffi.utils import colors
from daffi.utils.logger import get_daffi_logger
from daffi.exceptions import StopComponentError, ReckAcceptError

logger = get_daffi_logger(__name__, colors.red)


debug = os.getenv("DAFFI_DEBUG")


def write_exception_trace(retry_state):
    """Write exeption to file. For debug only purposes"""

    if not debug:
        return

    fn_name = retry_state.fn.__name__
    try:
        retry_state.outcome.result()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        root = Path(__file__).parents[2]
        with (root / "trace.txt").open("a") as f:
            f.write(f"{fn_name:-^60}\n{traceback.format_exc()}")
        raise


with_debug_trace = retry(
    stop=stop_after_attempt(1),
    reraise=True,
    retry_error_callback=write_exception_trace,
    retry=retry_if_not_exception_type(
        (EndOfStream, StopIteration, StopAsyncIteration, StopComponentError, AioRpcError, ReckAcceptError)
    ),
)
