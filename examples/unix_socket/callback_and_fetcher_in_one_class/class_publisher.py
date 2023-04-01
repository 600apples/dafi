"""
Publisher is the process that declares available remote functions
"""
import time
import logging
from daffi import Global
from daffi.registry import Fetcher, Callback


logging.basicConfig(level=logging.INFO)


class ShoppingList(Callback, Fetcher):
    def __post_init__(self):
        self.shopping_items = ["orange", "apple", "banana"]

    def first_item(self):
        return self.shopping_items[0]

    def iterate_shopping_list(self):
        for item in self.shopping_items:
            yield item


def main():
    sl = ShoppingList()

    # Process name is not required argument and will be generated automatically if not provided.
    with Global(init_controller=True, process_name="pub") as g:

        print("Wait for subscriber process to be started...")
        g.wait_process("sub")

        for _ in range(10):
            time.sleep(1)

            first_item = sl.first_item()
            print(f"First item in shopping list = {first_item}")

            for item in sl.iterate_shopping_list():
                print(f"Item from list :-> {item}")

        g.join()


if __name__ == "__main__":
    main()
