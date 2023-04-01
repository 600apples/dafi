import os
import logging
from itertools import cycle
from daffi import Global, BG
from daffi.decorators import callback, fetcher, __body_unknown__

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

colors = cycle(("red", "green", "blue"))


@fetcher(BG(return_result=False))
def send_email(title, content):
    """Fetcher proxy to `email processor`"""
    __body_unknown__(title, content)


@callback
async def colorize(title: str, content: str):
    color = next(colors)
    logger.warning(f"Color {color!r} has been chosen.")
    logger.warning(f"Calling a remote callback to send an email...")
    send_email(title=title, content=content)
    return color


if __name__ == "__main__":
    DAFI_PROCESS_NAME = "colorizer"
    DAFI_HOST = os.environ["DAFI_HOST"]
    DAFI_PORT = os.environ["DAFI_PORT"]
    DAFI_INIT_CONTROLLER = DAFI_HOST == DAFI_PROCESS_NAME

    g = Global(
        process_name=DAFI_PROCESS_NAME,
        host=DAFI_HOST,
        port=DAFI_PORT,
        init_controller=DAFI_INIT_CONTROLLER,
    )
    g.join()
