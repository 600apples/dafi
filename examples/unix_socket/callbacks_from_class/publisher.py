"""
Publisher is the process that declares available remote functions
"""
import logging
from daffi import Global, callback


logging.basicConfig(level=logging.INFO)


@callback
class Foo:
    """All public methods (without '_') become callbacks."""

    def __init__(self):
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
    # Init instance of "Foo" in order to have access to instance methods
    # (classmethods and staticmethods are available by default)
    Foo()

    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(init_controller=True)
    g.join()


if __name__ == "__main__":
    main()
