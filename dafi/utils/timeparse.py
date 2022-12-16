import re
from typing import Union


HOURS = r"(?P<hours>[\d.]+)h"
MINS = r"(?P<mins>[\d.]+)m"
SECS = r"(?P<secs>[\d.]+)s"
MILLI = r"(?P<milli>[\d.]+)ms"
MICRO = r"(?P<micro>[\d.]+)(?:us|µs)"
NANO = r"(?P<nano>[\d.]+)ns"


def opt(x):
    return r"(?:{x})?".format(x=x)


TIMEFORMAT = r"{HOURS}{MINS}{SECS}{MILLI}{MICRO}{NANO}".format(
    HOURS=opt(HOURS),
    MINS=opt(MINS),
    SECS=opt(SECS),
    MILLI=opt(MILLI),
    MICRO=opt(MICRO),
    NANO=opt(NANO),
)

MULTIPLIERS = {
    "hours": 60 * 60,
    "mins": 60,
    "secs": 1,
    "milli": 1.0 / 1000,
    "micro": 1.0 / 1000.0 / 1000,
    "nano": 1.0 / 1000.0 / 1000.0 / 1000.0,
}


def cast(value):
    return int(value) if value.isdigit() else float(value)


def timeparse(sval: str) -> Union[int, float]:
    """
    Parse a time expression, returning it as a number of seconds.  If
    possible, the return value will be an `int`; if this is not
    possible, the return will be a `float`.  Returns `None` if a time
    expression cannot be parsed from the given string.

    Arguments:
    - `sval`: the string value to parse

    >>> timeparse('1m24s')
    84
    >>> timeparse('1.2 minutes')
    72
    >>> timeparse('1.2 seconds')
    1.2
    """
    match = re.match(r"\s*" + TIMEFORMAT + r"\s*$", sval, re.I)
    if not match or not match.group(0).strip():
        return

    mdict = match.groupdict()
    return sum(MULTIPLIERS[k] * cast(v) for (k, v) in mdict.items() if v is not None)
