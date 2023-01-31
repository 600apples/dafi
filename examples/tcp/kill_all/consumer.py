"""
Consumer is the process that consumes available remote functions.
"""
import logging
import asyncio
from daffi import Global

logging.basicConfig(level=logging.INFO)


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(host="localhost", port=8888)
    g.join()
    g.stop()


if __name__ == "__main__":
    asyncio.run(main())
