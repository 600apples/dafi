"""
Consumer is the process that consumes available remote functions.
"""

import time
import logging
from datetime import datetime, timedelta
from daffi import Global, PERIOD

logging.basicConfig(level=logging.INFO)


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global()

    print("Wait for publisher process to be started...")
    g.wait_function("some_func")
    print("publisher process is running.")

    now = time.time()
    # Possible to pass timestamp or timedelta
    at_time = [now + 2, now + 10, datetime.now() + timedelta(seconds=15)]
    g.call.some_func() & PERIOD(at_time=at_time)  # another syntax: g.call.some_func().period(at_time=at_time)
    time.sleep(15)

    task = g.call.another_func(ts=3) & PERIOD(interval="3s")  # another syntax: g.call.some_func().period(period="3s")
    time.sleep(10)

    print("Canceling task...")
    task.cancel()
    time.sleep(1)

    # All scheduled tasks will be running even with transmitter process is terminated.
    g.stop()


if __name__ == "__main__":
    main()
