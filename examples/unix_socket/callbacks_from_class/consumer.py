"""
Consumer is the process that consumes available remote functions.
"""
import logging
import asyncio
from daffi import Global
from daffi.registry import Fetcher

logging.basicConfig(level=logging.INFO)


class Foo(Fetcher):
    """All public methods (without '_') become callbacks."""

    def __post_init__(self):
        self.internal_value = "my secret value"

    def method1(self):
        pass

    def method2(self, *args, **kwargs):
        pass

    @classmethod
    def class_method(cls, *args, **kwargs):
        pass

    @staticmethod
    def static_method(*args, **kwargs):
        pass


async def main():
    # Process name is not required argument and will be generated automatically if not provided.

    foo = Foo()

    with Global() as g:

        print("Wait for publisher process to be started...")
        g.wait_function("static_method")

        res = foo.static_method(foo="bar")
        print(res)

        res = foo.method1()
        print(res)

        res = foo.method2(foo="bar")
        print(res)


if __name__ == "__main__":
    asyncio.run(main())
