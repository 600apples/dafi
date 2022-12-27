"""
Publisher is the process that declares available remote functions
"""
import time
from dafi import Global, callback


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
    g = Global(init_controller=True, host="localhost", port=8888)
    time.sleep(120)
    print("Exit.")
    g.stop()


if __name__ == "__main__":
    main()
