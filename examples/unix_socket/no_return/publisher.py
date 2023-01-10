"""
Publisher is the process that declares available remote functions
"""
from daffi import Global, callback


@callback
async def some_func() -> None:
    """Used by 'consumer.py' process."""
    print("Hey! I was called remotely. Just want to notify you!")


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    Global(init_controller=True)

    # Note:
    #   This process will be running infinitely.


if __name__ == "__main__":
    main()
