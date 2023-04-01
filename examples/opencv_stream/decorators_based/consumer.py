"""
Consumer is the process that consumes available remote functions.
"""
import asyncio
import logging
import cv2
from daffi import Global
from daffi.decorators import fetcher, alias

logging.basicConfig(level=logging.INFO)


cap = cv2.VideoCapture(0)


@fetcher
@alias("show_stream")
def process_stream():
    """
    `process_stream` is stream generator for 3 remote processes `pub1`, `pub2` and `pub3`.
    Each of these processes has callback `show_stream`
    """
    ret, frame = cap.read()
    while ret:
        ret, frame = cap.read()
        yield frame


async def main():
    with Global() as g:

        print("wait all publishers")
        for proc in ("pub1", "pub2", "pub3"):
            # Wait all stream processors
            g.wait_process(proc)

        process_stream()

    # After the loop release the cap object
    cap.release()
    # Destroy all the windows
    cv2.destroyAllWindows()


if __name__ == "__main__":
    asyncio.run(main())
