import pytest
import sys
from daffi import BROADCAST
from daffi.decorators import __body_unknown__, fetcher
from subprocess import Popen


expected = {
    "broadcast-node-1": "broadcast-node-1.broadcast_test",
    "broadcast-node-4": "broadcast-node-4.broadcast_test",
    "broadcast-node-3": "broadcast-node-3.broadcast_test",
    "broadcast-node-5": "broadcast-node-5.broadcast_test",
    "broadcast-node-6": "broadcast-node-6.broadcast_test",
    "broadcast-node-7": "broadcast-node-7.broadcast_test",
    "broadcast-node-8": "broadcast-node-8.broadcast_test",
    "broadcast-node-2": "broadcast-node-2.broadcast_test",
    "broadcast-node-9": "broadcast-node-9.broadcast_test",
}


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix(remote_callbacks_path, g, stop_components):
    @fetcher(BROADCAST(return_result=True, timeout=10))
    def broadcast_callback(value: str):
        __body_unknown__(value)

    g = g()
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            template_name="broadcast.jinja2",
            process_name=f"broadcast-node-{i}",
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"broadcast-node-{i}")

    try:
        result = broadcast_callback(value="broadcast_test")
        assert result == expected
    finally:
        await stop_components(remotes, g)


async def test_callback_per_node_tcp(remote_callbacks_path, g, stop_components):
    @fetcher(BROADCAST(return_result=True, timeout=10))
    def broadcast_callback(value: str):
        __body_unknown__(value)

    g = g(host="localhost")
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            template_name="broadcast.jinja2",
            process_name=f"broadcast-node-{i}",
            host="localhost",
            port=g.port,
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"broadcast-node-{i}")

    try:
        result = broadcast_callback(value="broadcast_test")
        assert result == expected
    finally:
        await stop_components(remotes, g)
