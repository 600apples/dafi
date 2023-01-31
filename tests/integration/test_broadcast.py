import pytest
import sys
from daffi import BROADCAST, __body_unknown__, fetcher
from subprocess import Popen


expected = {
    "node-1": "node-1.broadcast_test",
    "node-4": "node-4.broadcast_test",
    "node-3": "node-3.broadcast_test",
    "node-5": "node-5.broadcast_test",
    "node-6": "node-6.broadcast_test",
    "node-7": "node-7.broadcast_test",
    "node-8": "node-8.broadcast_test",
    "node-2": "node-2.broadcast_test",
    "node-9": "node-9.broadcast_test",
}


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix(remote_callbacks_path, g):
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
            process_name=f"node-{i}",
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    result = broadcast_callback(value="broadcast_test")
    assert result == expected

    g.stop(True)
    [p.terminate() for p in remotes]


async def test_callback_per_node_tcp(remote_callbacks_path, g):
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
            process_name=f"node-{i}",
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

    result = broadcast_callback(value="broadcast_test")
    assert result == expected
    g.stop(True)
    [p.terminate() for p in remotes]
