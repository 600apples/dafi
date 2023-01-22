"""
Consumer is the process that consumes available remote functions.
"""

import time
import logging
from daffi import Global, GlobalContextError, NO_RETURN

logging.basicConfig(level=logging.INFO)


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global()

    print("Wait for publisher process to be started...")
    g.wait_function("some_func")

    try:
        while True:
            # We dont expect any return value here.
            print("Call some_func on publisher")
            g.call.some_func() & NO_RETURN  # another syntax: g.call.some_func().no_return()
            time.sleep(5)

    except GlobalContextError as e:
        print(e)


if __name__ == "__main__":
    main()
