"""
Consumer is the process that consumes available remote functions.
"""
import logging
import time
from datetime import datetime, timedelta
from daffi import Global, PERIOD
from daffi.decorators import fetcher

logging.basicConfig(level=logging.INFO)

now = datetime.utcnow().timestamp()
# Possible to pass timestamp or timedelta
at_time = [now + 2, now + 10, datetime.utcnow() + timedelta(seconds=15)]


@fetcher(PERIOD(at_time=at_time))
def some_func():
    pass


@fetcher(PERIOD(interval="3s"))
def another_func(ts):
    pass


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(host="localhost", port=8888)

    print("Wait for publisher process to be started...")
    g.wait_function("some_func")
    print("publisher process is running.")

    some_func()
    time.sleep(15)

    task = another_func(ts=3)
    time.sleep(10)
    task.cancel()
    time.sleep(1)

    # All scheduled tasks will be running even with transmitter process is terminated.
    g.stop()


if __name__ == "__main__":
    main()
