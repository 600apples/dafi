import logging
from trio import sleep, run
from daffi import Global, callback, RemoteStoppedUnexpectedly

logging.basicConfig(level=logging.INFO)

PROC_NAME = "Trio Brown Fox"


@callback
async def cheers1(arg1, arg2):
    return f"Cheers from {PROC_NAME!r} process. You called function cheers1 with arguments: arg1={arg1}, arg2={arg2}"


@callback
async def cheers2():
    return f"Cheers from {PROC_NAME!r} process. You called function cheers2"


async def main():
    remote_proc = "Trio White Rabbit"
    g = Global(process_name=PROC_NAME)

    print(f"wait for {remote_proc} process to be started...")
    await g.wait_process(remote_proc)

    for _ in range(10):
        try:
            res = g.call.greeting1("foo", "bar").fg()  # another syntax: g.call.greeting1("foo", "bar") & FG
            print(res)
            await sleep(2)

            res = g.call.greeting2().fg()
            print(res)
            await sleep(2)
        except RemoteStoppedUnexpectedly as e:
            # We need to handle RemoteStoppedUnexpectedly in order one process exit earlier.
            # It means remote callbacks becomes unavailable.
            print(e)
            break

    g.stop()


if __name__ == "__main__":
    run(main)
