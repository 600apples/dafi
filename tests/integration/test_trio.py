import pytest
import sys
from daffi import FG, BG
from subprocess import Popen


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix(remote_callbacks_path, g):
    g = g()

    executable_files = [
        remote_callbacks_path(
            template_name="test_trio.jinja2",
            process_name=f"test-node",
        )
    ]
    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))

    g.wait_process(f"test-node")

    res = g.call.test_callback() & FG(timeout="7s")
    assert res == "Ok"

    res = await g.call.test_callback() & FG(timeout="7s")
    assert res == "Ok"

    res = await g.call.test_callback() & BG(eta=2)
    res = await res.get_async()
    assert res == "Ok"

    [p.terminate() for p in remotes]
