import logging
from trio import run, sleep
from daffi import Global
from daffi.exceptions import RemoteStoppedUnexpectedly
from daffi.decorators import callback, fetcher


logging.basicConfig(level=logging.INFO)

PROC_NAME = "Trio White Rabbit"


@callback
@fetcher
async def greeting1(arg1, arg2):
    return (
        f"Greeting from {PROC_NAME!r} process. You called function greeting1 with arguments: arg1={arg1}, arg2={arg2}"
    )


@callback
@fetcher
async def greeting2():
    return f"Greeting from {PROC_NAME!r} process. You called function greeting2"


async def main():
    remote_proc = "Trio Brown Fox"
    g = Global(process_name=PROC_NAME, init_controller=True)

    print(f"wait for {remote_proc} process to be started...")
    g.wait_process(remote_proc)

    for _ in range(10):
        try:
            res = await greeting1("foo", "bar")
            print(res)
            await sleep(2)
            res = await greeting2()
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
