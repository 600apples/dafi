"""
Consumer is the process that consumes available remote functions.
"""

import time
from datetime import datetime, timedelta
from dafi import Global, PERIOD


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(host="localhost", port=8888)

    print("Wait for publisher process to be started...")
    g.wait_function("some_func")
    print("publisher process is running.")

    now = time.time()
    # Possible to pass timestamp or timedelta
    at_time = [now + 2, now + 10, datetime.now() + timedelta(seconds=15)]
    g.call.some_func() & PERIOD(at_time=at_time)  # another syntax: g.call.some_func().period(at_time=at_time)

    time.sleep(15)

    g.call.another_func(ts=5) & PERIOD(period="5s")  # another syntax: g.call.some_func().period(period="3s")

    # All scheduled tasks will be running even with transmitter process is terminated.
    g.stop()


if __name__ == "__main__":
    main()
