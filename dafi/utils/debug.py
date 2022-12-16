import traceback
from pathlib import Path


from anyio import EndOfStream
from tenacity import retry, stop_after_attempt, retry_if_not_exception_type


def write_exception_trace(retry_state):
    """Write exeption to file. For debug only purposes"""

    fn_name = retry_state.fn.__name__
    try:
        retry_state.outcome.result()
    except Exception:
        root = Path(__file__).parents[2]
        with (root / "trace.txt").open("a") as f:
            f.write(f"{fn_name:-^60}\n{traceback.format_exc()}")
        raise


with_debug_trace = retry(
    stop=stop_after_attempt(1),
    reraise=True,
    retry_error_callback=write_exception_trace,
    retry=retry_if_not_exception_type(EndOfStream),
)
