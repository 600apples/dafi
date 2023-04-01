"""
Publisher is the process that declares available remote functions
"""
import logging
from daffi import Global
from daffi.registry import Callback

logging.basicConfig(level=logging.INFO)


class Items(Callback):
    def iterate_items(self, item):
        """Used by 'consumer.py' process."""
        print(f"Received item from fetcher: {item}")

    def iterate_items_with_args(self, index, item):
        print(f"Received item from fetcher: Item={item}, index={index}")


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(init_controller=True)
    g.join()


if __name__ == "__main__":
    main()
