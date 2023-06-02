import re
import sys
import asyncio
import logging
from pathlib import Path

import pytest
from sniffio import current_async_library_cvar

# Extend python import path to get src package from here
sys.path += [str(Path(__file__).resolve().parents[1])]

# anyio has contextvars support, In order to mock anio backend we need to set backend explicitly
current_async_library_cvar.set("asyncio")

logging.basicConfig(level=logging.INFO)


def pytest_collection_modifyitems(items):
    """This is the same as using the @pytest.mark.anyio on all test functions in the module"""

    for item in items:
        item.add_marker("asyncio")


@pytest.fixture(autouse=True)
def skip_if_marker_not_present(checkmarker, request):
    """
    Execute test if makerker <xyz> provided explicitly while starting tests for classes like

    @pytest.mark.abc
    @pytest.mark.<xyz>
    @pytest.mark.skip_if_marker_not_present('<xyz>')
    class TestSuite: ..
    """

    # Get list of all markers specified before test execution
    markers = list(map(str.strip, re.split("and|or", checkmarker)))
    if request.node.get_closest_marker("skip_if_marker_not_present"):
        if request.node.get_closest_marker("skip_if_marker_not_present").args[0] not in markers:
            pytest.skip("This test can only be run if you specify its marker explicitly at startup")


@pytest.fixture
def checkmarker(pytestconfig):
    """
    Get raw list of markers from execution params
    ! It might be something like 'markerA and markerB or markerC'
    """

    markers_arg = pytestconfig.getoption("-m")
    return markers_arg or ""


@pytest.fixture(scope="session")
def event_loop(request):
    """
    Create an instance of the default event loop for each test case.
    It helps to fix issue with different event loops in different test cases
    according to https://github.com/pytest-dev/pytest-asyncio/issues/38
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
