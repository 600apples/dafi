"""
Publisher is the process that declares available remote functions
"""
import time
from daffi import Global, callback


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

    def method_with_access_to_g_object(self, g):
        return f"triggered method_with_access_to_g_object: G = {g}"


def main():
    # Init instance of "Foo" in order to have access to instance methods
    # (classmethods and staticmethods are available by default)
    Foo()

    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(init_controller=True, host="localhost", port=8888)
    time.sleep(120)
    print("Exit.")
    g.stop()


if __name__ == "__main__":
    main()
