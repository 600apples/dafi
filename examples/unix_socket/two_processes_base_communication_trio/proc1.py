from trio import sleep, run
from dafi import Global, callback

PROC_NAME = "Trio White Rabbit"


@callback
async def greeting1(arg1, arg2):
    return f"Greeting from {PROC_NAME!r} process. You called function greeting1 with arguments: arg1{arg1}, arg2={arg2}"


@callback
async def greeting2():
    return f"Greeting from {PROC_NAME!r} process. You called function greeting2"


async def main():
    g = Global(process_name=PROC_NAME)

    while True:
        g.cheers1("foo", "bar")
        await sleep(2)
        g.cheers2()
        await sleep(2)


if __name__ == "__main__":
    run(main)
