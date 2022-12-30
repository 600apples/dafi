import pytest
from pathlib import Path
import socketserver

from dafi import Global
from jinja2 import FileSystemLoader, Environment


THIS = Path(__file__)

templateLoader = FileSystemLoader(searchpath=THIS.parent / "templates")
templateEnv = Environment(loader=templateLoader)


@pytest.fixture
def remote_callbacks_path(tmp_path):
    def dec(**kwargs):
        """
        Expected arguments:
           start_range: int
           end_range: int
           process_name: str
           host: str
           port: int
        """
        file = tmp_path / "main.py"
        template = templateEnv.get_template("many_callbacks.jinja2")
        output = template.render(**kwargs)
        file.write_text(output)
        return file

    return dec


@pytest.fixture
def g():
    """Create Global object. Global is singleton and should be cleaned before each test suite."""
    Global._instances.clear()
    gl = None

    def dec(host: str = None, port: int = None):
        nonlocal gl
        gl = Global(init_controller=True, host=host, port=port)
        return gl

    try:
        yield dec
    finally:
        gl.stop()


@pytest.fixture
def free_port() -> int:

    with socketserver.TCPServer(("localhost", 0), None) as s:
        free_port = s.server_address[1]
        return free_port
