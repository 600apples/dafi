import time
import logging
from daffi import Global, RemoteStoppedUnexpectedly
from daffi.decorators import callback, fetcher


logging.basicConfig(level=logging.INFO)

PROC_NAME = "White Rabbit"


@callback
@fetcher
def greeting1(arg1, arg2):
    return (
        f"Greeting from {PROC_NAME!r} process. You called function greeting1 with arguments: arg1={arg1}, arg2={arg2}"
    )


@callback
@fetcher
def greeting2():
    return f"Greeting from {PROC_NAME!r} process. You called function greeting2"


def main():
    remote_proc = "Brown Fox"
    g = Global(process_name=PROC_NAME, init_controller=True)

    print(f"wait for {remote_proc} process to be started...")
    g.wait_process(remote_proc)

    for _ in range(10):
        try:
            res = greeting1("foo", "bar")
            print(res)
            time.sleep(2)
            res = greeting2()
            print(res)
            time.sleep(2)
        except RemoteStoppedUnexpectedly as e:
            # We need to handle GlobalContextError in order one process exit earlier.
            # It means remote callbacks becomes unavailable.
            print(e)
            break

    g.stop()


if __name__ == "__main__":
    main()
