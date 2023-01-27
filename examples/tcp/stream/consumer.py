"""
Consumer is the process that consumes available remote functions.
"""
import time
import logging
from daffi import Global, STREAM, fetcher, __body_unknown__

logging.basicConfig(level=logging.INFO)

@fetcher
async def process_stream(item) -> None:
    __body_unknown__(item)


def iterator():
    for i in range(1, 10000):
        yield i
        time.sleep(2)


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(host="localhost", port=8888, init_controller=True)

    print("Wait for publisher process to be started...")
    g.wait_function("process_stream")

    process_stream(iterator()) & STREAM

    g.stop()



if __name__ == "__main__":
    main()
