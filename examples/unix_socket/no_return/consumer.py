"""
Consumer is the process that consumes available remote functions.
"""

import time
import logging
from daffi import Global, GlobalContextError, BG
from daffi.decorators import fetcher

logging.basicConfig(level=logging.INFO)


@fetcher(BG(no_return=True))
def some_func():
    pass


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global()

    print("Wait for publisher process to be started...")
    g.wait_function("some_func")

    try:
        while True:
            # We dont expect any return value here.
            print("Call some_func on publisher")
            some_func()
            time.sleep(5)

    except GlobalContextError as e:
        print(e)


if __name__ == "__main__":
    main()
