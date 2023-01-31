import pytest
import sys
import logging
from daffi import STREAM, FG, fetcher, __body_unknown__
from subprocess import Popen

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_stream_per_node_unix(remote_callbacks_path, g):
    g = g()
    start_range = 1
    end_range = 6
    range_ = list(range(start_range, end_range))
    stream_values = list(range(int(1e3)))

    @fetcher(STREAM, args_from_body=True)
    def process_stream():
        for i in stream_values:
            yield i

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

    try:
        process_stream()
        for i in range_:
            processed_arr = getattr(g.call, f"stream_result_{i}")() & FG(timeout=10)
            assert set(stream_values) == set(processed_arr)
        g.stop(True)

    finally:
        [p.terminate() for p in remotes]


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_stream_per_node_unix_using_remote_decorator(remote_callbacks_path, g):
    @fetcher
    def process_stream(stream):
        __body_unknown__(stream)

    g = g()
    start_range = 1
    end_range = 6
    range_ = list(range(start_range, end_range))
    stream_values = list(range(int(1e3)))

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

    try:
        process_stream(stream_values) & STREAM

        for i in range_:
            processed_arr = getattr(g.call, f"stream_result_{i}")() & FG(timeout=10)
            assert set(stream_values) == set(processed_arr)

        g.stop(True)
    finally:
        [p.terminate() for p in remotes]


async def test_stream_per_node_tcp(remote_callbacks_path, g):
    @fetcher(STREAM)
    def process_stream(stream):
        __body_unknown__(stream)

    g = g(host="localhost")
    start_range = 1
    end_range = 6
    range_ = list(range(start_range, end_range))
    stream_values = list(range(int(1e3)))

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

    try:
        process_stream(stream_values)

        for i in range_:
            processed_arr = getattr(g.call, f"stream_result_{i}")() & FG(timeout=10)
            assert set(stream_values) == set(processed_arr)

        g.stop(True)
    finally:
        [p.terminate() for p in remotes]
