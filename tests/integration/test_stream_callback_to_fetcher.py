import pytest
import sys
import logging
from subprocess import Popen
from daffi import BG
from daffi.registry import Fetcher, Args, Callback

from daffi.decorators import alias

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_stream_callback_to_fetcher_per_node_unix(remote_callbacks_path, g):
    g = g()
    start_range = 1
    end_range = 6
    range_ = list(range(start_range, end_range))

    class StreamReceiver(Fetcher):
        def process_stream(self, end: int):
            return end

        def process_class_stream(self, end: int):
            return end

        @alias("process_stream")
        def process_stream_with_alias(self, end: int):
            end = end * 2
            return Args(end=end)

        @alias("process_class_stream")
        def process_class_stream_with_alias(self, end: int):
            end = end * 2
            return Args(end=end)

    class StreamReceiverWithCallback(Fetcher, Callback):
        def process_stream(self, end: int):
            return end

        def process_class_stream(self, end: int):
            return end

        @alias("process_stream")
        def process_stream_with_alias(self, end: int):
            end = end * 2
            return Args(end=end)

        @alias("process_class_stream")
        def process_class_stream_with_alias(self, end: int):
            end = end * 2
            return Args(end=end)

    stream_receiver = StreamReceiver()
    stream_with_callback_receiver = StreamReceiverWithCallback()

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            template_name="stream_from_callback.jinja2",
            process_name=f"node-{i}",
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    try:
        res = stream_receiver.process_stream(1000)

        assert set(res) == set(range(1000))

        # Execute with different exec modifier
        res = stream_receiver.process_class_stream.call(exec_modifier=BG(timeout=10, eta=2), end=2000)
        assert set(res) == set(range(2000))

        res = stream_receiver.process_stream_with_alias(1000)
        assert set(res) == set(range(2000))

        # Execute with different exec modifier
        res = stream_receiver.process_class_stream_with_alias.call(exec_modifier=BG(timeout=10, eta=2), end=2000)
        assert set(res) == set(range(4000))

        # Test Fetcher and Callback parents. Fetcher method must be proxy in this case
        res = stream_with_callback_receiver.process_stream(1000)

        assert set(res) == set(range(1000))

        # Execute with different exec modifier
        res = stream_with_callback_receiver.process_class_stream.call(exec_modifier=BG(timeout=10, eta=2), end=2000)
        assert set(res) == set(range(2000))

        res = stream_with_callback_receiver.process_stream_with_alias(1000)
        assert set(res) == set(range(1000))

        # Execute with different exec modifier
        res = stream_with_callback_receiver.process_class_stream_with_alias.call(
            exec_modifier=BG(timeout=10, eta=2), end=2000
        )
        assert set(res) == set(range(2000))

    finally:
        [p.terminate() for p in remotes]


async def test_stream_callback_to_fetcher_per_node_tcp(remote_callbacks_path, g):
    g = g(host="localhost")
    start_range = 1
    end_range = 6
    range_ = list(range(start_range, end_range))

    class StreamReceiver(Fetcher):
        def process_stream(self, end: int):
            return end

        def process_class_stream(self, end: int):
            return end

        @alias("process_stream")
        def process_stream_with_alias(self, end: int):
            return Args(end=end)

        @alias("process_class_stream")
        def process_class_stream_with_alias(self, end: int):
            return Args(end=end)

    stream_receiver = StreamReceiver()

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            template_name="stream_from_callback.jinja2",
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

    try:
        res = stream_receiver.process_stream(1000)
        assert set(res) == set(range(1000))

        # Execute with different exec modifier
        res = stream_receiver.process_class_stream.call(exec_modifier=BG(timeout=10, eta=2), end=2000)
        assert set(res) == set(range(2000))

        res = stream_receiver.process_stream_with_alias(1000)
        assert set(res) == set(range(1000))

        # Execute with different exec modifier
        res = stream_receiver.process_class_stream_with_alias.call(exec_modifier=BG(timeout=10, eta=2), end=2000)
        assert set(res) == set(range(2000))

    finally:
        [p.terminate() for p in remotes]
