"""
Consumer is the process that consumes available remote functions.
Make sure you started publisher.py first in order to transfer and execute function on this process.
"""
import logging
import os
from daffi import Global

logging.basicConfig(level=logging.INFO)

PROC_NAME = "Brown Fox"


async def func_to_transfer():
    """Return pid id of remote process"""
    import os

    return os.getpid()


def main():
    remote_proc = "White Rabbit"

    with Global(host="localhost", port=8888) as g:
        print(f"wait for {remote_proc} process to be started...")
        g.wait_process(remote_proc)

        print(f"My pid: {os.getpid()}")

        remote_pid = g.transfer_and_call(remote_process=remote_proc, func=func_to_transfer)
        print(f"Pid of White Rabbit process: {remote_pid}")


if __name__ == "__main__":
    main()
