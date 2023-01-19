"""
Consumer is the process that consumes available remote functions.
"""
import asyncio
import cv2
from daffi import Global, STREAM

cap = cv2.VideoCapture(0)


def frame_iterator():
    ret, frame = cap.read()
    while ret:
        ret, frame = cap.read()
        yield frame


async def main():
    with Global() as g:

        for proc in ("process1", "process2"):
            g.wait_process(proc)

        print("Wait for publisher process to be started...")
        g.call.show_stream(frame_iterator()) & STREAM

    # After the loop release the cap object
    cap.release()
    # Destroy all the windows
    cv2.destroyAllWindows()


if __name__ == "__main__":
    asyncio.run(main())
