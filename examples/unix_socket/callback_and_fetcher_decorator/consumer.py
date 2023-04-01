"""
Consumer is the process that consumes available remote functions.
"""

import logging
import asyncio
from daffi import Global
from daffi.registry import Callback, Fetcher
from daffi.decorators import callback, fetcher

logging.basicConfig(level=logging.INFO)


class RemoteGroup(Callback, Fetcher):
    async def do_something(self, a: int):
        return f"Received number: {a}"


@callback
@fetcher
async def my_func(a: int):
    return f"Received number: {a}"


async def main():

    rm = RemoteGroup()

    g = Global(process_name="proc1")
    await g.wait_process_async("proc2")

    for _ in range(10):
        result = await rm.do_something(5)
        print(result)

        result = await my_func(10)
        print(result)

    await g.join_async()


if __name__ == "__main__":
    asyncio.run(main())
