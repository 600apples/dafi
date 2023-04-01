"""
Consumer is the process that consumes available remote functions.
"""
import time
import asyncio
import logging
from daffi import Global, BG
from daffi.registry import Fetcher, Args

logging.basicConfig(level=logging.INFO)


class Items(Fetcher):
    def __post_init__(self):
        self.items = ["bread", "eggs", "orange", "chicken"]

    def iterate_items(self):
        """Used by 'consumer.py' process."""
        for item in self.items:
            yield item
            time.sleep(2)

    def iterate_items_with_args(self):
        for index, item in enumerate(self.items):
            yield Args(index=index, item=item)
            time.sleep(2)


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    with Global() as g:

        print("Wait for publisher process to be started...")
        g.wait_function("iterate_items")

        items = Items()
        print("Iterate without Args")
        items.iterate_items()

        print("Iterate with Args")
        items.iterate_items_with_args()

        print("Iterate in background mode")
        items.iterate_items.call(exec_modifier=BG)

        for i in range(10):
            print(f"Do parallel job: item={i}")
            time.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
