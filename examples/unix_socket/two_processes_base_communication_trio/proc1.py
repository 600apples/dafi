from trio import sleep, run
from daffi import Global, callback, RemoteStoppedUnexpectedly

PROC_NAME = "Trio White Rabbit"


@callback
async def greeting1(arg1, arg2):
    return (
        f"Greeting from {PROC_NAME!r} process. You called function greeting1 with arguments: arg1={arg1}, arg2={arg2}"
    )


@callback
async def greeting2():
    return f"Greeting from {PROC_NAME!r} process. You called function greeting2"


async def main():
    remote_proc = "Trio Brown Fox"
    g = Global(process_name=PROC_NAME, init_controller=True)

    print(f"wait for {remote_proc} process to be started...")
    await g.wait_process(remote_proc)

    for _ in range(10):
        try:
            res = g.call.cheers1("foo", "bar").fg()  # another syntax: g.call.cheers1("foo", "bar") & FG
            print(res)
            await sleep(2)

            res = g.call.cheers2().fg()
            print(res)
            await sleep(2)
        except RemoteStoppedUnexpectedly as e:
            # We need to handle GlobalContextError in order one process exit earlier.
            # It means remote callbacks becomes unavailable.
            print(e)
            break

    g.stop()


if __name__ == "__main__":
    run(main)
