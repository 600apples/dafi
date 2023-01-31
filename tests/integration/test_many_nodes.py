import sys
import pytest
import asyncio
from random import choice
from subprocess import Popen
from daffi import FG, BG, BROADCAST, fetcher, __body_unknown__


async def call_remote(g, _range, exec_type):
    for _ in range(5):
        node_num = choice(_range)
        callback_name = f"get_process_name{node_num}"
        expected_process_name = f"node-{node_num}"
        future = getattr(g.call, callback_name)()

        if exec_type == FG:
            res = future & FG(timeout=60)
        elif exec_type == BG:
            future = future & BG(timeout=60)
            res = future.get()

        assert res == expected_process_name


async def call_remote_broadcast(path):
    @fetcher(BROADCAST(return_result=True, timeout=60))
    async def get_process_name1(path):
        __body_unknown__(path)

    for i in range(5):
        res = await get_process_name1(path=path / str(i))
        assert set(res) == {
            "node-1",
            "node-4",
            "node-3",
            "node-6",
            "node-7",
            "node-5",
            "node-8",
            "node-2",
            "node-9",
        }


@pytest.mark.parametrize("exec_type", [FG, BG])
@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix(remote_callbacks_path, exec_type, g):
    g = g()
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py", callback_index=i, template_name="callback_per_node.jinja2", process_name=f"node-{i}"
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    await asyncio.gather(*[call_remote(g, range_, exec_type) for _ in range(500)])
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
            callback_index=i,
            template_name="callback_per_node.jinja2",
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

    await asyncio.gather(*[call_remote(g, range_, exec_type) for _ in range(500)])
    g.stop(True)
    [p.terminate() for p in remotes]


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_broadcast_unix(remote_callbacks_path, g):
    g = g()
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py", callback_index=1, template_name="callback_per_node.jinja2", process_name=f"node-{i}"
        )
        for i in range_
    ]

    path = executable_files[0].parent / "test_data"
    path.mkdir()

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    await asyncio.gather(*[call_remote_broadcast(path=path) for _ in range(500)])

    all_files = list(path.iterdir())
    assert len(all_files) == 5

    all_processes = [f"node-{i}" for i in range_]
    for file in all_files:
        file_text = file.read_text()
        for proc_name in all_processes:
            assert proc_name in file_text

    g.stop(True)
    [p.terminate() for p in remotes]


async def test_callback_per_node_broadcast_tcp(remote_callbacks_path, g):
    g = g(host="localhost")
    start_range = 1
    end_range = 10
    range_ = list(range(start_range, end_range))

    executable_files = [
        remote_callbacks_path(
            filename=f"{i}.py",
            callback_index=1,
            template_name="callback_per_node.jinja2",
            process_name=f"node-{i}",
            host="localhost",
            port=g.port,
        )
        for i in range_
    ]

    path = executable_files[0].parent / "test_data"
    path.mkdir()

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    await asyncio.gather(*[call_remote_broadcast(path=path) for _ in range(500)])

    all_files = list(path.iterdir())
    assert len(all_files) == 5

    all_processes = [f"node-{i}" for i in range_]
    for file in all_files:
        file_text = file.read_text()
        for proc_name in all_processes:
            assert proc_name in file_text

    g.stop(True)
    [p.terminate() for p in remotes]
