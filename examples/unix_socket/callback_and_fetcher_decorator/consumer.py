"""
Consumer is the process that consumes available remote functions.
"""

import logging
import asyncio
from daffi import Global, FG, callback_and_fetcher

logging.basicConfig(level=logging.INFO)


@callback_and_fetcher
class RemoteGroup:
    def do_something(self, a: int):
        return f"Received number: {a}"


@callback_and_fetcher
def my_func(a: int):
    return f"Received number: {a}"


async def main():

    rm = RemoteGroup()

    g = Global(process_name="proc1")
    await g.wait_process_async("proc2")

    for _ in range(10):
        result = rm.do_something(5) & FG
        print(result)

        result = my_func(10) & FG
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
