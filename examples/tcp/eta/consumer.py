"""
Consumer is the process that consumes available remote functions.
"""

import time
import asyncio
from daffi import Global, GlobalContextError, BG, UnableToFindCandidate


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(host="localhost", port=8888)

    print("Wait for publisher process to be started...")
    g.wait_function("add")

    for _ in range(10):

        print("Publisher is up and running. 'add' function execution..")
        start = time.time()

        # bg returns instance of AsyncResult.
        try:

            ares = g.call.add(5, 15) & BG(eta=5)

            # Simulate long running job
            time.sleep(2)
            print("Job finished")

            # Here execution stuck for additional 3 seconds due to eta
            res = ares.get()
            if res:
                print(f"Calculated result = {res}. Total exec time: {time.time() - start}")

        except (GlobalContextError, UnableToFindCandidate) as e:
            print(e)
            break

    g.stop()


if __name__ == "__main__":
    asyncio.run(main())
