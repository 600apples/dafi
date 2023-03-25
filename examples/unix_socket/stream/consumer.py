"""
Consumer is the process that consumes available remote functions.
"""
import time
import logging
from daffi import Global
from daffi.decorators import fetcher

logging.basicConfig(level=logging.INFO)


@fetcher
def process_stream():
    for i in range(1, 10000):
        yield i
        time.sleep(2)


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(init_controller=True)

    print("Wait for publisher process to be started...")
    g.wait_function("process_stream")

    process_stream()
    g.stop()


if __name__ == "__main__":
    main()
