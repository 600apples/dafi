import pytest
import sys
from datetime import datetime
from daffi import PERIOD, GlobalContextError, RemoteCallError
from subprocess import Popen


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets dont work on windows")
async def test_callback_per_node_unix(remote_callbacks_path, g):
    g = g()
    now = datetime.utcnow().timestamp()

    executable_files = [
        remote_callbacks_path(
            template_name="period1.jinja2",
            process_name=f"test-node",
        )
    ]

    remotes = []
    for exc in executable_files:
        remotes.append(Popen([sys.executable, exc]))

    g.wait_process(f"test-node")

    with pytest.raises(GlobalContextError) as ex_context:
        g.call.cb1() & PERIOD(interval="10s")

    assert "[ test_callback, func1, func2 ]" in str(ex_context)

    task1 = g.call.test_callback() & PERIOD(interval="20s")
    registered_tasks = g.get_scheduled_tasks(remote_process="test-node")
    assert registered_tasks == [{"condition": "period", "func_name": "test_callback", "uuid": task1.uuid}]
    task1.cancel()
    registered_tasks = g.get_scheduled_tasks(remote_process="test-node")
    assert registered_tasks == []

    # Cancel by id
    task2 = g.call.test_callback() & PERIOD(interval="20s")
    registered_tasks = g.get_scheduled_tasks(remote_process="test-node")
    assert registered_tasks == [{"condition": "period", "func_name": "test_callback", "uuid": task2.uuid}]
    assert g.cancel_scheduled_task_by_uuid(remote_process="test-node", uuid=task1.uuid) is False
    with pytest.raises(RemoteCallError):
        g.cancel_scheduled_task_by_uuid(remote_process="test-node", uuid=1234567890)
    registered_tasks = g.get_scheduled_tasks(remote_process="test-node")
    assert registered_tasks == [{"condition": "period", "func_name": "test_callback", "uuid": task2.uuid}]
    g.cancel_scheduled_task_by_uuid(remote_process="test-node", uuid=task2.uuid)
    registered_tasks = g.get_scheduled_tasks(remote_process="test-node")
    assert registered_tasks == []

    g.stop(True)
    [p.terminate() for p in remotes]
