import time
from dafi import Global, callback

PROC_NAME = "White Rabbit"


@callback
def greeting1(arg1, arg2):
    return f"Greeting from {PROC_NAME!r} process. You called function greeting1 with arguments: arg1{arg1}, arg2={arg2}"


@callback
def greeting2():
    return f"Greeting from {PROC_NAME!r} process. You called function greeting2"


def main():
    g = Global(process_name=PROC_NAME)

    while True:
        g.cheers1("foo", "bar")
        time.sleep(2)
        g.cheers2()
        time.sleep(2)


if __name__ == "__main__":
    main()
