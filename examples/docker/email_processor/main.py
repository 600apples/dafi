import os
import logging
from daffi import Global, callback


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@callback
def send_email(title: str, content: str):
    logger.warning("-" * 20)
    logger.warning(f"Email has been sent:\nemail title = {title}, email content = {content}")
    logger.warning("-" * 20)


if __name__ == "__main__":
    DAFI_PROCESS_NAME = "email_processor"
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
