import time
from daffi import Global, callback, RemoteStoppedUnexpectedly, GlobalContextError

PROC_NAME = "Brown Fox"


@callback
def cheers1(arg1, arg2):
    return f"Cheers from {PROC_NAME!r} process. You called function cheers1 with arguments: arg1={arg1}, arg2={arg2}"


@callback
def cheers2():
    return f"Cheers from {PROC_NAME!r} process. You called function cheers2"


def main():
    remote_proc = "White Rabbit"
    g = Global(process_name=PROC_NAME)

    print(f"wait for {remote_proc} process to be started...")
    g.wait_process(remote_proc)

    for _ in range(20):
        try:
            res = g.call.greeting1("foo", "bar").fg()
            print(res)
            time.sleep(2)
            res = g.call.greeting2().fg()
            print(res)
            time.sleep(2)
        except RemoteStoppedUnexpectedly as e:
            # We need to handle GlobalContextError in order one process exit earlier.
            # It means remote callbacks becomes unavailable.
            print(e)
            break
        except GlobalContextError:
            time.sleep(10)
            continue

    g.stop()


if __name__ == "__main__":
    main()
