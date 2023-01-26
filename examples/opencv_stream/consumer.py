"""
Consumer is the process that consumes available remote functions.
"""
import asyncio
import logging
import cv2
from daffi import Global, STREAM, fetcher

logging.basicConfig(level=logging.INFO)


cap = cv2.VideoCapture(0)


@fetcher(STREAM, args_from_body=True)
def show_stream():
    ret, frame = cap.read()
    while ret:
        ret, frame = cap.read()
        yield frame


async def main():
    with Global() as g:

        for proc in ("pub1", "pub2", "pub3"):
            g.wait_process(proc)

        print("Wait for publisher process to be started...")
        show_stream()

    # After the loop release the cap object
    cap.release()
    # Destroy all the windows
    cv2.destroyAllWindows()


if __name__ == "__main__":
    asyncio.run(main())
