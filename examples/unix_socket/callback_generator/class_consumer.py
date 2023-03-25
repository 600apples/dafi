"""
Consumer is the process that consumes available remote functions.
"""
import asyncio
import logging
from daffi import Global, BG
from daffi.registry import Fetcher

logging.basicConfig(level=logging.INFO)


class Items(Fetcher):
    def iterate_items(self):
        """Used by 'consumer.py' process."""
        pass


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    with Global() as g:

        print("Wait for publisher process to be started...")
        g.wait_function("iterate_items")

        items = Items()

        print("Iterate in background mode")
        res = items.iterate_items.call(exec_modifier=BG)
        for item in res:
            print(item)

        print("Iterate in foreground mode")
        for item in items.iterate_items():
            print(item)


if __name__ == "__main__":
    asyncio.run(main())
