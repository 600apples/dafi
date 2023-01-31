import sys
import logging
from typing import Callable
from typing import Any

from daffi.utils import colors
from daffi.utils.settings import DEBUG

logging.getLogger("grpc._cython.cygrpc").setLevel(logging.ERROR)


class DaffiLoggerAdapter(logging.LoggerAdapter):
    def debug(self, msg: Any, *args, **kwargs) -> None:
        if DEBUG:
            return self.info(msg=msg, *args, **kwargs)
        return super().debug(msg=msg, *args, **kwargs)


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


def get_daffi_logger(name: str, color: Callable):
    logger = logging.getLogger(name=name)
    root_level = logging.getLogger().getEffectiveLevel()

    cho = logging.StreamHandler(sys.stdout)
    che = logging.StreamHandler(sys.stderr)

    logger.propagate = False
    if logger.hasHandlers():
        logger.handlers.clear()

    cho.addFilter(lambda record: record.levelno <= logging.INFO)
    delim = color("|")
    logger = DaffiLoggerAdapter(logger, {"app": f"{delim} {logger.name:10} {delim}"})

    logger.setLevel(root_level)
    cho.setLevel(root_level)
    che.setLevel(logging.WARNING)
    formatter = ColoredFormatter("%(app)s %(message)s")
    cho.setFormatter(formatter)
    che.setFormatter(formatter)

    logger.logger.addHandler(cho)
    logger.logger.addHandler(che)
    return logger
