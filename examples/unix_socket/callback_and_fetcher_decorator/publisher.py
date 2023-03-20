"""
Publisher is the process that declares available remote functions
"""
import logging
from daffi import Global
from daffi.registry import Callback, Fetcher
from daffi.decorators import callback, fetcher

logging.basicConfig(level=logging.INFO)


class RemoteGroup(Callback, Fetcher):
    def do_something(self, a: int):
        return a + 10


@callback
@fetcher
def my_func(a: int):
    return a + 10


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    rm = RemoteGroup()

    g = Global(init_controller=True, process_name="proc2")
    g.wait_process("proc1")

    for _ in range(10):
        result = rm.do_something(5)
        print(result)

        result = my_func(10)
        print(result)


if __name__ == "__main__":
    main()
