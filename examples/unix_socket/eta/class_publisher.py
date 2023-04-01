"""
Publisher is the process that declares available remote functions
"""
import logging
from daffi import Global
from daffi.registry import Callback

logging.basicConfig(level=logging.INFO)


class Calculator(Callback):
    def add(self, arg1: int, arg2: int) -> int:
        """Used by 'consumer.py' process."""
        return arg1 + arg2

    def subtract(self, arg1: int, arg2: int) -> int:
        """Not used. Just for example."""
        return arg1 - arg2


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(init_controller=True)
    g.join()


if __name__ == "__main__":
    main()
