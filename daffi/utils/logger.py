import sys
import logging
from typing import Callable
from daffi.utils import colors


ch = logging.StreamHandler(sys.stdout)


class Formatter(logging.Formatter):
    def format(self, record):
        return super(Formatter, self).format(record)


class ColoredFormatter(logging.Formatter):
    """A logging.Formatter which prints colored WARNING and ERROR messages"""

    def get_level_message(self, record):
        if record.levelno >= logging.ERROR:
            return colors.red(record.levelname)
        if record.levelno >= logging.WARNING:
            return colors.yellow(record.levelname)
        return record.levelname

    def format(self, record):
        if isinstance(record.msg, bytes):
            record.msg = record.msg.decode("utf-8")
        message = super().format(record)
        return f"{self.get_level_message(record)} {message}"


def patch_logger(logger: logging.Logger, color: Callable):
    logger.propagate = False
    logger = logging.LoggerAdapter(logger, {"app": color(f"[[ {logger.name} ]]")})

    logger.setLevel(logging.DEBUG)

    ch.setLevel(logging.DEBUG)
    formatter = ColoredFormatter("%(app)s: %(message)s")
    ch.setFormatter(formatter)
    logger.logger.addHandler(ch)
    return logger
