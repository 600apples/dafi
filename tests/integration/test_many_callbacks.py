import sys
from dafi import Global
from subprocess import Popen

async def test_many_callbacks(remote_callbacks_path, global_obj):
    process_name = "test_node"
    start_range = 1
    end_range = 5000

    executable_file = remote_callbacks_path(process_name=process_name, start_range=start_range, end_range=end_range)

    try:
        remote = Popen([sys.executable, executable_file])

        global_obj.wait_function("callback_func_199")
        res = global_obj.call.callback_func_199(1,2,3).fg()
    finally:
        remote.kill()
