"""
Publisher is the process that declares available remote functions
"""
import logging
from daffi import Global
from daffi.decorators import callback

logging.basicConfig(level=logging.INFO)


@callback
async def process_stream(item) -> None:
    """Used by 'consumer.py' process."""
    print(f"item received: {item}")


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    Global().join()

    # Note:
    #   This process will be running infinitely.


if __name__ == "__main__":
    main()
