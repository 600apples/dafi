import pytest
import sys
from daffi import STREAM, FG
from subprocess import Popen


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix(remote_callbacks_path, g):
    g = g()
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))
    stream_values = list(range(int(1e4)))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            template_name="stream.jinja2",
            process_name=f"node-{i}",
            cb_name=i,
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    await g.call.process_stream(stream_values) & STREAM

    for i in range_:
        processed_arr = await getattr(g.call, f"stream_result_{i}")() & FG
        assert stream_values == processed_arr

    res = g.stop(True)
    assert res == {
        "node-6": None,
        "node-1": None,
        "node-5": None,
        "node-7": None,
        "node-4": None,
        "node-3": None,
        "node-2": None,
        "node-9": None,
        "node-8": None,
    }


async def test_callback_per_node_tcp(remote_callbacks_path, g):
    g = g(host="localhost")
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))
    stream_values = list(range(int(1e4)))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            template_name="stream.jinja2",
            process_name=f"node-{i}",
            cb_name=i,
            host="localhost",
            port=g.port,
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    await g.call.process_stream(stream_values) & STREAM

    for i in range_:
        processed_arr = await getattr(g.call, f"stream_result_{i}")() & FG
        assert stream_values == processed_arr

    res = g.stop(True)
    assert res == {
        "node-6": None,
        "node-1": None,
        "node-5": None,
        "node-7": None,
        "node-4": None,
        "node-3": None,
        "node-2": None,
        "node-9": None,
        "node-8": None,
    }