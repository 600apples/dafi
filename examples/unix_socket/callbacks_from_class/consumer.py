"""
Consumer is the process that consumes available remote functions.
"""

import asyncio
from daffi import Global, FG


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    with Global() as g:

        print("Wait for publisher process to be started...")
        g.wait_function("static_method")

        res = g.call.static_method(foo="bar") & FG
        print(res)

        res = g.call.method1() & FG
        print(res)

        res = g.call.method2(foo="bar") & FG
        print(res)


if __name__ == "__main__":
    asyncio.run(main())
