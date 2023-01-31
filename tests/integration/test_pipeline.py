import pytest
import sys
from daffi import FG, BG
from subprocess import Popen


@pytest.mark.parametrize("exec_type", [FG, BG])
@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix(remote_callbacks_path, exec_type, g):
    g = g()
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            cb_name=f"cb{i}",
            next_cb_name=f"cb{i + 1}",
            last=len(range_) == i,
            template_name="pipeline.jinja2",
            process_name=f"node-{i}",
            exec_type=exec_type.__name__,
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    value = g.call.cb1() & FG
    assert value == "My secret value"

    g.stop(True)
    [p.terminate() for p in remotes]


@pytest.mark.parametrize("exec_type", [FG, BG])
async def test_callback_per_node_tcp(remote_callbacks_path, exec_type, g):
    g = g(host="localhost")
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            cb_name=f"cb{i}",
            next_cb_name=f"cb{i + 1}",
            last=len(range_) == i,
            template_name="pipeline.jinja2",
            process_name=f"node-{i}",
            exec_type=exec_type.__name__,
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

    value = g.call.cb1() & FG
    assert value == "My secret value"

    g.stop(True)
    [p.terminate() for p in remotes]


@pytest.mark.parametrize("exec_type", [FG, BG])
@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix_async(remote_callbacks_path, exec_type, g):
    g = g()
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            cb_name=f"cb{i}",
            next_cb_name=f"cb{i + 1}",
            last=len(range_) == i,
            template_name="pipeline2.jinja2",
            process_name=f"node-{i}",
            exec_type=exec_type.__name__,
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    value = g.call.cb1() & FG
    assert value == "My secret value"

    g.stop(True)
    [p.terminate() for p in remotes]


@pytest.mark.parametrize("exec_type", [FG, BG])
async def test_callback_per_node_tcp_async(remote_callbacks_path, exec_type, g):
    g = g(host="localhost")
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            cb_name=f"cb{i}",
            next_cb_name=f"cb{i + 1}",
            last=len(range_) == i,
            template_name="pipeline2.jinja2",
            process_name=f"node-{i}",
            exec_type=exec_type.__name__,
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

    value = g.call.cb1() & FG
    assert value == "My secret value"

    g.stop(True)
    [p.terminate() for p in remotes]
