# This file contains miscellaneous functions that are used in the project.
from typing import Tuple, Union


def calc_sleep_and_backoff(step: float, max_: Union[int, float]) -> callable:
    def _wrapper(sleep: float, backoff: int) -> Tuple[float, int]:
        if sleep >= max_:
            return sleep, backoff
        backoff -= 1
        if backoff == 0:
            sleep += step
            backoff = round(1 / sleep)
        return sleep, backoff

    return _wrapper
