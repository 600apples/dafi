from asyncio import sleep, run
from dafi import Global, callback, GlobalContextError

PROC_NAME = "Async White Rabbit"


@callback
async def greeting1(arg1, arg2):
    return (
        f"Greeting from {PROC_NAME!r} process. You called function greeting1 with arguments: arg1={arg1}, arg2={arg2}"
    )


@callback
async def greeting2():
    return f"Greeting from {PROC_NAME!r} process. You called function greeting2"


async def main():
    remote_proc = "Async Brown Fox"
    g = Global(process_name=PROC_NAME)

    print(f"wait for {remote_proc} process to be started...")
    await g.wait_process(remote_proc)

    try:
        while True:
            res = g.call.cheers1("foo", "bar").fg()  # another syntax: g.call.cheers1("foo", "bar") & FG
            print(res)
            await sleep(2)
            res = g.call.cheers2().fg()
            print(res)
            await sleep(2)
    except GlobalContextError as e:
        print(e)


if __name__ == "__main__":
    run(main())
