"""
Publisher is the process that declares available remote functions
"""
import logging
from daffi import Global, callback

logging.basicConfig(level=logging.INFO)


@callback
async def on_stream(item: int) -> None:
    """Used by 'consumer.py' process."""
    print(f"Processing item: {item}")


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(init_controller=True)
    g.join()


if __name__ == "__main__":
    main()
