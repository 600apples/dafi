import sys
import time
import asyncio
from random import choices, choice
from subprocess import Popen

import pytest
from dafi import FG, BG, PERIOD, NO_RETURN


timings = []
remote_type = "callback_func_", "async_callback_func_"


async def call_remote(g, _range, exec_type):
    random_args = [1, 2, 134566, "12345", "867", ("a", "b", "c"), {"foo": "bar"}, object]

    for i in range(5):

        func_args = tuple(choices(random_args, k=4))
        start = time.time()
        future = getattr(g.call, remote_type[bool(i % 2)] + str(choice(_range)))(*func_args)

        if exec_type == FG:
            res = future & FG
            assert func_args == res[0]
            assert {} == res[1]
            assert res[2].startswith("test_node")
            timings.append(time.time() - start)

        elif exec_type == BG:
            future = future & BG
            res = future.get()

            assert res

            assert func_args == res[0]
            assert {} == res[1]
            assert res[2].startswith("test_node")
            timings.append(time.time() - start)


async def call_remote_no_return(g, _range, exec_type, path):

    for i in range(5):

        func_args = dict(path=str(path), text=str(i))
        start = time.time()
        future = getattr(g.call, remote_type[bool(i % 2)] + str(choice(_range)))(**func_args)

        if exec_type == PERIOD:
            future = future & PERIOD(at_time=start + 2)
            await asyncio.sleep(3)
            future.cancel()
        elif exec_type == NO_RETURN:
            future & NO_RETURN
            await asyncio.sleep(2)


@pytest.mark.parametrize("exec_type", [BG, FG])
@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_many_callbacks_unix(remote_callbacks_path, exec_type, g):
    g = g()
    process_name = "test_node"
    start_range = 1
    end_range = 1000
    range_ = list(range(start_range, end_range))
    executable_file = remote_callbacks_path(
        template_name="many_callbacks.jinja2", process_name=process_name, start_range=start_range, end_range=end_range
    )

    try:
        Popen([sys.executable, executable_file])
        g.wait_process(process_name)
        await asyncio.gather(*[call_remote(g, range_, exec_type) for _ in range(500)])

        max_time = max(timings)
        assert max_time < 1

    finally:
        g.stop(True)


@pytest.mark.parametrize("exec_type", [PERIOD, NO_RETURN])
@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_many_callbacks_unix_no_return(remote_callbacks_path, exec_type, g):
    g = g()
    process_name = "test_node"
    start_range = 1
    end_range = 1000
    range_ = list(range(start_range, end_range))
    executable_file = remote_callbacks_path(
        template_name="many_callbacks.jinja2",
        process_name=process_name,
        start_range=start_range,
        end_range=end_range,
        write_to_file=True,
    )
    path = executable_file.parent / "test_data"
    path.mkdir()

    try:
        Popen([sys.executable, executable_file])
        g.wait_process(process_name)
        await asyncio.gather(*[call_remote_no_return(g, range_, exec_type, path / str(i)) for i in range(500)])

        all_files = list(path.iterdir())
        assert len(all_files) == 500
        for file in all_files:
            assert file.read_text() == "4"
    finally:
        g.stop(True)


@pytest.mark.parametrize("exec_type", [BG, FG])
async def test_many_callbacks_tcp(remote_callbacks_path, exec_type, g, free_port):
    g = g(host="localhost", port=free_port)
    process_name = "test_node"
    start_range = 1
    end_range = 1000
    range_ = list(range(start_range, end_range))
    executable_file = remote_callbacks_path(
        template_name="many_callbacks.jinja2",
        process_name=process_name,
        start_range=start_range,
        end_range=end_range,
        host="localhost",
        port=free_port,
    )
    try:
        Popen([sys.executable, executable_file])
        g.wait_process(process_name)
        await asyncio.gather(*[call_remote(g, range_, exec_type) for _ in range(500)])
        max_time = max(timings)
        assert max_time < 1
    finally:
        g.stop(True)


@pytest.mark.parametrize("exec_type", [PERIOD, NO_RETURN])
async def test_many_callbacks_tcp_no_return(remote_callbacks_path, exec_type, g, free_port):
    g = g(host="0.0.0.0", port=free_port)
    process_name = "test_node"
    start_range = 1
    end_range = 1000
    range_ = list(range(start_range, end_range))
    executable_file = remote_callbacks_path(
        template_name="many_callbacks.jinja2",
        process_name=process_name,
        start_range=start_range,
        end_range=end_range,
        write_to_file=True,
        host="0.0.0.0",
        port=free_port,
    )
    path = executable_file.parent / "test_data"
    path.mkdir()

    try:
        Popen([sys.executable, executable_file])
        g.wait_process(process_name)
        await asyncio.gather(*[call_remote_no_return(g, range_, exec_type, path / str(i)) for i in range(500)])

        all_files = list(path.iterdir())
        assert len(all_files) == 500
        for file in all_files:
            assert file.read_text() == "4"
    finally:
        g.stop(True)
