"""
Publisher is the process that declares available remote functions
"""
import logging
from daffi import Global, callback

logging.basicConfig(level=logging.INFO)


@callback
async def add(arg1: int, arg2: int) -> int:
    """Used by 'consumer.py' process."""
    return arg1 + arg2


@callback
def subtract(arg1: int, arg2: int) -> int:
    """Not used. Just for example."""
    return arg1 - arg2


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    Global(init_controller=True, host="localhost", port=8888).join()


if __name__ == "__main__":
    main()
