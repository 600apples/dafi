"""
Consumer is the process that consumes available remote functions.
"""
import logging
import asyncio
from daffi import Global, FG

logging.basicConfig(level=logging.INFO)


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    with Global(host="localhost", port=8888) as g:

        print("Wait for publisher process to be started...")
        g.wait_function("static_method")

        res = g.call.static_method(foo="bar") & FG
        print(res)

        res = g.call.method1() & FG
        print(res)

        res = g.call.method2(foo="bar") & FG
        print(res)

        res = g.call.method_with_access_to_g_object() & FG
        print(res)


if __name__ == "__main__":
    asyncio.run(main())
