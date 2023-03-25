"""
Publisher is the process that declares available remote functions
"""
import logging
import time
from daffi import Global
from daffi.registry import Callback

logging.basicConfig(level=logging.INFO)


class Foo(Callback):
    """All public methods (without '_') become callbacks."""

    def __post_init__(self):
        self.internal_value = "my secret value"

    def method1(self):
        return f"triggered method1. Internal value: {self.internal_value}"

    def method2(self, *args, **kwargs):
        return f"triggered method2 with arguments: {args}, {kwargs}"

    @classmethod
    def class_method(cls, *args, **kwargs):
        return f"triggered classmethod with arguments: {args}, {kwargs}"

    @staticmethod
    def static_method(*args, **kwargs):
        return f"triggered staticmethod with arguments: {args}, {kwargs}"


def main():

    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(init_controller=True)
    time.sleep(120)
    print("Exit.")
    g.stop()


if __name__ == "__main__":
    main()
