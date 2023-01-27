"""
Consumer is the process that consumes available remote functions.
"""
import logging
import time
import asyncio
from daffi import Global, BG, fetcher, __body_unknown__

logging.basicConfig(level=logging.INFO)


@fetcher
def add(a: int, b: int):
    __body_unknown__(a, b)


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    with Global() as g:

        print("Wait for publisher process to be started...")
        g.wait_function("add")

        for _ in range(10):

            print("Publisher is up and running. 'add' function execution..")
            start = time.time()

            # bg returns instance of AsyncResult.
            ares = add(5, 15) & BG(eta=5)

            # Simulate long running job
            time.sleep(2)
            print("Job finished")

            # Here execution stuck for additional 3 seconds due to eta
            res = ares.get()
            if res:
                print(f"Calculated result = {res}. Total exec time: {time.time() - start}")


if __name__ == "__main__":
    asyncio.run(main())
