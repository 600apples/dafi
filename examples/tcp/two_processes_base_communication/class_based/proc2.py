import time
import logging
from daffi import Global
from daffi.exceptions import RemoteStoppedUnexpectedly
from daffi.registry import Callback, Fetcher, __body_unknown__

logging.basicConfig(level=logging.INFO)

PROC_NAME = "Brown Fox"


class GreetingsCallback(Callback):
    auto_init = True

    def greeting1(self, arg1, arg2):
        return (
            f"Cheers from {PROC_NAME!r} process. You called function cheers1 with arguments: arg1={arg1}, arg2={arg2}"
        )

    def greeting2(self):
        return f"Cheers from {PROC_NAME!r} process. You called function cheers2"


class Greetings(Fetcher):
    def greeting1(self, arg1, arg2):
        __body_unknown__(arg1, arg2)

    def greeting2(self):
        __body_unknown__()


def main():
    gret = Greetings()
    remote_proc = "White Rabbit"
    g = Global(process_name=PROC_NAME, host="localhost", port=8888)

    print(f"wait for {remote_proc} process to be started...")
    g.wait_process(remote_proc)

    for _ in range(10):
        try:
            res = gret.greeting1("foo", "bar")
            print(res)
            time.sleep(2)
            res = gret.greeting2()
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
