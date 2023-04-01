import pytest
import sys
from daffi import FG, BG, GlobalContextError
from daffi.decorators import fetcher, __body_unknown__
from subprocess import Popen


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix(remote_callbacks_path, g):
    g = g()

    @fetcher(FG(timeout="7s"))
    async def test_callback():
        __body_unknown__()

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

    res = await test_callback()
    assert res == "Ok"

    res = await test_callback.call(exec_modifier=BG(eta=2))
    res = await res.get_async()
    assert res == "Ok"

    res = await test_callback.call(exec_modifier=FG(timeout="7s"))
    assert res == "Ok"

    res = await test_callback.call(exec_modifier=BG(eta=2))
    res = await res.get_async()
    assert res == "Ok"

    [p.terminate() for p in remotes]


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix_wrong_body_args(remote_callbacks_path, g):
    g = g()

    @fetcher(FG(timeout="7s"))
    async def test_callback(a, b):
        return a, b

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

    with pytest.raises(GlobalContextError):
        await test_callback(a="foo", b="bar")

    with pytest.raises(GlobalContextError):
        await test_callback.call(a="foo", b="bar", exec_modifier=BG(eta=2))

    [p.terminate() for p in remotes]
