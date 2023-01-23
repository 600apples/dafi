"""
Publisher is the process that declares available remote functions
"""
import logging
from daffi import Global, callback

logging.basicConfig(level=logging.INFO)


@callback
async def some_func() -> None:
    """Used by 'consumer.py' process."""
    print("some_func triggered!!!")


@callback
async def another_func(ts) -> None:
    """Used by 'consumer.py' process."""
    print(f"another_func triggered (period = {ts} sec)!!!")


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    Global(init_controller=True, host="localhost", port=8888).join()

    # Note:
    #   This process will be running infinitely.


if __name__ == "__main__":
    main()
