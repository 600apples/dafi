from trio import sleep, run
from dafi import Global, callback

PROC_NAME = "Trio Brown Fox"


@callback
async def cheers1(arg1, arg2):
    return f"Cheers from {PROC_NAME!r} process. You called function cheers1 with arguments: arg1{arg1}, arg2={arg2}"


@callback
async def cheers2():
    return f"Cheers from {PROC_NAME!r} process. You called function cheers2"


async def main():
    g = Global(process_name=PROC_NAME)

    while True:
        g.greeting1("foo", "bar")
        await sleep(2)
        g.greeting2()
        await sleep(2)


if __name__ == "__main__":
    run(main)
