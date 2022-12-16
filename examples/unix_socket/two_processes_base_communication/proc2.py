import time
from dafi import Global, callback

PROC_NAME = "Brown Fox"


@callback
def cheers1(arg1, arg2):
    return f"Cheers from {PROC_NAME!r} process. You called function cheers1 with arguments: arg1{arg1}, arg2={arg2}"


@callback
def cheers2():
    return f"Cheers from {PROC_NAME!r} process. You called function cheers2"


def main():
    g = Global(process_name=PROC_NAME)

    while True:
        g.greeting1("foo", "bar")
        time.sleep(2)
        g.greeting2()
        time.sleep(2)


if __name__ == "__main__":
    main()
