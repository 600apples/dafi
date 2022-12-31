"""
Consumer is the process that consumes available remote functions.
"""

import asyncio
from dafi import Global


async def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(host="localhost", port=8888)
    g.join()


if __name__ == "__main__":
    asyncio.run(main())
