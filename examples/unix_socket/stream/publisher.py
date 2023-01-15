"""
Publisher is the process that declares available remote functions
"""
from daffi import Global, callback


@callback
async def on_stream(item: int) -> None:
    """Used by 'consumer.py' process."""
    print(f"Processing item: {item}")
    return item


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(init_controller=True)
    g.join()


if __name__ == "__main__":
    main()
