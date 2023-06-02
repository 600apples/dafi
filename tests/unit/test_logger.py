import logging
from logging import LoggerAdapter

import pytest
from daffi.utils import colors
from daffi.utils.logger import get_daffi_logger, ColoredFormatter


class MockLevelRecord:
    levelno = None
    levelname = None
    msg = None
    exc_info = None
    exc_text = None
    stack_info = None

    def __init__(self, levelno, levelname, msg=""):
        self.levelno = levelno
        self.levelname = levelname
        self.msg = msg

    def getMessage(self):
        return self.msg


class TestConsoleWarningFormatter:
    @pytest.mark.parametrize(
        "record, expected_result",
        [
            (MockLevelRecord(logging.ERROR, "error"), f"{colors.red('error')}"),
            (
                MockLevelRecord(logging.WARNING, "warning"),
                f"{colors.yellow('warning')}",
            ),
            (MockLevelRecord(logging.INFO, "info"), "info"),
        ],
    )
    async def test_get_level_message(self, record, expected_result):
        # Preparation
        formatter = ColoredFormatter()

        # Execution
        result = formatter.get_level_message(record)

        # Assertion
        assert result == expected_result

    @pytest.mark.parametrize(
        "record, expected_result",
        [
            (
                MockLevelRecord(logging.ERROR, "error", "error"),
                f"{colors.red('error')} error",
            ),
            (
                MockLevelRecord(logging.WARNING, "warning", b"warning"),
                f"{colors.yellow('warning')} warning",
            ),
        ],
    )
    async def test_format(self, record, expected_result):
        # Preparation
        formatter = ColoredFormatter()

        # Execution
        result = formatter.format(record)

        # Assertion
        assert result == expected_result

    async def test_patch_logging(self):
        # Preparation
        logger = get_daffi_logger(__name__, color=colors.red)

        # Assertion
        assert isinstance(logger, LoggerAdapter)
