"""
Consumer is the process that consumes available remote functions.
"""
import asyncio
import logging
import cv2
from daffi import Global
from daffi.registry import Fetcher
from daffi.decorators import alias, local


logging.basicConfig(level=logging.INFO)


class StreamProcessor(Fetcher):
    def __post_init__(self):
        self.cap = cv2.VideoCapture(0)

    @alias("show_stream")
    def process_stream(self):
        """
        `process_stream` is stream generator for 3 remote processes `pub1`, `pub2` and `pub3`.
        Each of these processes has callback `show_stream`
        """
        ret, frame = self.cap.read()
        while ret:
            ret, frame = self.cap.read()
            yield frame

    @local
    def destroy(self):
        """Method decorated with `local` are private and not registered as fetchers"""
        self.cap.release()
        cv2.destroyAllWindows()


async def main():
    with Global() as g:

        stream_processor = StreamProcessor()

        print("wait all publishers")
        for proc in ("pub1", "pub2", "pub3"):
            # Wait all stream processors
            g.wait_process(proc)

        stream_processor.show_stream()

    # After the loop release the cap object
    stream_processor.destroy()


if __name__ == "__main__":
    asyncio.run(main())
