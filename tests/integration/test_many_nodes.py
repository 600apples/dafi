import sys
import pytest
import asyncio
from random import choice
from subprocess import Popen
from dafi import FG, BG, BROADCAST


async def call_remote(g, _range, exec_type):
    for _ in range(5):
        node_num = choice(_range)
        callback_name = f"get_process_name{node_num}"
        expected_process_name = f"node-{node_num}"
        future = getattr(g.call, callback_name)()

        if exec_type == FG:
            res = future & FG
        elif exec_type == BG:
            future = future & BG
            res = future.get()

        assert res == expected_process_name


async def call_remote_broadcast(g, path):
    for i in range(5):
        callback_name = f"get_process_name1"
        res = getattr(g.call, callback_name)(path=path / str(i)) & BROADCAST(return_result=True)
        assert res == {
            "node-3": "node-3",
            "node-1": "node-1",
            "node-5": "node-5",
            "node-6": "node-6",
            "node-4": "node-4",
            "node-8": "node-8",
            "node-2": "node-2",
            "node-9": "node-9",
            "node-7": "node-7",
        }
    await asyncio.sleep(1)


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

    res = g.stop(True)
    assert res == {
        "node-4": None,
        "node-1": None,
        "node-2": None,
        "node-6": None,
        "node-7": None,
        "node-3": None,
        "node-5": None,
        "node-8": None,
        "node-9": None,
    }


@pytest.mark.parametrize("exec_type", [FG, BG])
async def test_callback_per_node_tcp(remote_callbacks_path, exec_type, g, free_port):
    g = g(host="localhost", port=free_port)
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
            port=free_port,
        )
        for i in range_
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))
    for i in range_:
        g.wait_process(f"node-{i}")

    await asyncio.gather(*[call_remote(g, range_, exec_type) for _ in range(500)])

    res = g.stop(True)
    assert res == {
        "node-4": None,
        "node-1": None,
        "node-2": None,
        "node-6": None,
        "node-7": None,
        "node-3": None,
        "node-5": None,
        "node-8": None,
        "node-9": None,
    }


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

    await asyncio.gather(*[call_remote_broadcast(g, path=path) for _ in range(500)])

    all_files = list(path.iterdir())
    assert len(all_files) == 5

    all_processes = [f"node-{i}" for i in range_]
    for file in all_files:
        file_text = file.read_text()
        for proc_name in all_processes:
            assert proc_name in file_text

    res = g.stop(True)
    assert res == {
        "node-1": None,
        "node-4": None,
        "node-3": None,
        "node-6": None,
        "node-7": None,
        "node-5": None,
        "node-8": None,
        "node-2": None,
        "node-9": None,
    }


async def test_callback_per_node_broadcast_tcp(remote_callbacks_path, g, free_port):
    g = g(host="localhost", port=free_port)
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
            port=free_port,
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

    await asyncio.gather(*[call_remote_broadcast(g, path=path) for _ in range(500)])

    all_files = list(path.iterdir())
    assert len(all_files) == 5

    all_processes = [f"node-{i}" for i in range_]
    for file in all_files:
        file_text = file.read_text()
        for proc_name in all_processes:
            assert proc_name in file_text

    res = g.stop(True)
    assert res == {
        "node-1": None,
        "node-4": None,
        "node-3": None,
        "node-6": None,
        "node-7": None,
        "node-5": None,
        "node-8": None,
        "node-2": None,
        "node-9": None,
    }
