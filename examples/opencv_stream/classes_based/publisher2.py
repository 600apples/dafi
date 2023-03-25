"""
Publisher is the process that declares available remote functions
"""
import cv2
import logging
from daffi import Global
from daffi.registry import Callback

logging.basicConfig(level=logging.INFO)


class StreamDisplay(Callback):
    async def show_stream(self, frame: int) -> None:
        """Used by 'consumer.py' process."""
        # Display the resulting frame
        cv2.imshow("daffi publisher2", frame)
        cv2.waitKey(1)


def main():
    # Process name is not required argument and will be generated automatically if not provided.

    g = Global(process_name="pub2")
    g.join()


if __name__ == "__main__":
    main()
