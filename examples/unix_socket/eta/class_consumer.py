"""
Consumer is the process that consumes available remote functions.
"""
import asyncio
import logging
from daffi import Global
from daffi.registry import Fetcher

logging.basicConfig(level=logging.INFO)


class Calculator(Fetcher):
    async def add(self, arg1: int, arg2: int) -> int:
        """Used by 'consumer.py' process."""
        pass

    def subtract(self, arg1: int, arg2: int) -> int:
        """Not used. Just for example."""
        pass


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    with Global() as g:

        print("Wait for publisher process to be started...")
        g.wait_function("add")

        calculator = Calculator()

        for _ in range(10):
            res = await calculator.add(5, 15)
            print(f"Calculated result = {res}")


if __name__ == "__main__":
    asyncio.run(main())
