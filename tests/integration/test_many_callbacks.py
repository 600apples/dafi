import sys
import time
import asyncio
from datetime import datetime, timedelta
from random import choices, choice
from subprocess import Popen

import pytest
from dafi import FG, BG, PERIOD


timings = []


async def call_remote(g, _range, exec_type):
    remote_type = "callback_func_", "async_callback_func_"
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
            assert func_args == res[0]
            assert {} == res[1]
            assert res[2].startswith("test_node")
            timings.append(time.time() - start)

        elif exec_type == PERIOD:
            at_time = [start + 2, start + 10, datetime.now() + timedelta(seconds=15)]
            future = future & PERIOD(at_time=at_time)
            await asyncio.sleep(7)
            future.cancel()
            await asyncio.sleep(1)


@pytest.mark.parametrize("exec_type", [FG, BG, PERIOD])
@pytest.mark.skipif(sys.platform == 'win32', reason="Unix sockets dont work on windows")
async def test_many_callbacks(remote_callbacks_path, exec_type, g):

    process_name = "test_node"
    start_range = 1
    end_range = 1000
    remote = None
    range_ = list(range(start_range, end_range))
    executable_file = remote_callbacks_path(process_name=process_name, start_range=start_range, end_range=end_range)

    try:
        remote = Popen([sys.executable, executable_file])
        g.wait_process(process_name)
        await asyncio.gather(*[call_remote(g, range_, exec_type) for _ in range(500)])

        max_time = max(timings)
        assert max_time < 1

    finally:
        if remote:
            remote.kill()
        g.stop()
