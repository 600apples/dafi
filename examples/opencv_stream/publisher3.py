"""
Publisher is the process that declares available remote functions
"""
import cv2
import logging
from daffi import Global, callback

logging.basicConfig(level=logging.INFO)


@callback
async def show_stream(frame: int) -> None:
    """Used by 'consumer.py' process."""
    # Display the resulting frame
    cv2.imshow("daffi publisher3", frame)
    cv2.waitKey(1)


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(process_name="pub3")
    g.join()


if __name__ == "__main__":
    main()
