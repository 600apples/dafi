import time
from dafi import Global, callback, GlobalContextError

PROC_NAME = "White Rabbit"


@callback
def greeting1(arg1, arg2):
    return (
        f"Greeting from {PROC_NAME!r} process. You called function greeting1 with arguments: arg1={arg1}, arg2={arg2}"
    )


@callback
def greeting2():
    return f"Greeting from {PROC_NAME!r} process. You called function greeting2"


def main():
    remote_proc = "Brown Fox"
    g = Global(process_name=PROC_NAME)

    print(f"wait for {remote_proc} process to be started...")
    g.wait_process(remote_proc)

    try:
        while True:
            res = g.call.cheers1("foo", "bar").fg()
            print(res)
            time.sleep(2)
            res = g.call.cheers2().fg()
            print(res)
            time.sleep(2)
    except GlobalContextError as e:
        print(e)


if __name__ == "__main__":
    main()
