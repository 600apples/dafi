"""
Consumer is the process that consumes available remote functions.
"""

import time
import logging
import asyncio
from daffi import Global, STREAM

logging.basicConfig(level=logging.INFO)


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    with Global() as g:

        print("Wait for publisher process to be started...")
        g.wait_function("on_stream")

        start = time.time()
        stream_items = range(int(1e5))

        g.call.on_stream(stream_items) & STREAM
        print(f"Stream finished in {time.time() - start}")


if __name__ == "__main__":
    asyncio.run(main())
