"""
Publisher is the process that declares available remote functions
"""
import time
import logging
from daffi import Global
from daffi.registry import Callback

logging.basicConfig(level=logging.INFO)


class Items(Callback):
    def __post_init__(self):
        self.items = ["bread", "eggs", "orange", "chicken"]

    def iterate_items(self):
        """Used by 'consumer.py' process."""
        for item in self.items:
            yield item
            time.sleep(2)


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(init_controller=True)
    g.join()


if __name__ == "__main__":
    main()
