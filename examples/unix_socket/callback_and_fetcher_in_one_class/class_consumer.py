"""
Consumer is the process that consumes available remote functions.
"""
import time
import asyncio
import logging
from daffi import Global
from daffi.registry import Fetcher, Callback

logging.basicConfig(level=logging.INFO)


class ShoppingList(Callback, Fetcher):
    def __post_init__(self):
        self.shopping_items = ["duck", "pork"]

    async def first_item(self):
        return self.shopping_items[0]

    def iterate_shopping_list(self):
        for item in self.shopping_items:
            yield item


async def main():
    sl = ShoppingList()

    # Process name is not required argument and will be generated automatically if not provided.
    with Global(process_name="sub") as g:

        print("Wait for publisher process to be started...")
        g.wait_process("pub")

        for _ in range(10):
            time.sleep(1)

            first_item = await sl.first_item()

            print(f"First item in shopping list = {first_item}")

            for item in sl.iterate_shopping_list():
                print(f"Item from list :-> {item}")


if __name__ == "__main__":
    asyncio.run(main())
