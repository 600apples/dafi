import logging
from trio import run, sleep
from daffi import Global, RemoteStoppedUnexpectedly
from daffi.registry import Callback, Fetcher

logging.basicConfig(level=logging.INFO)

PROC_NAME = "Trio White Rabbit"


class Greetings(Callback, Fetcher):
    async def greeting1(self, arg1, arg2):
        return f"Greeting from {PROC_NAME!r} process. You called function greeting1 with arguments: arg1={arg1}, arg2={arg2}"

    async def greeting2(self):
        return f"Greeting from {PROC_NAME!r} process. You called function greeting2"


async def main():
    gret = Greetings()
    remote_proc = "Trio Brown Fox"
    g = Global(process_name=PROC_NAME, init_controller=True)

    print(f"wait for {remote_proc} process to be started...")
    g.wait_process(remote_proc)

    for _ in range(10):
        try:
            res = await gret.greeting1("foo", "bar")
            print(res)
            await sleep(2)
            res = await gret.greeting2()
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
