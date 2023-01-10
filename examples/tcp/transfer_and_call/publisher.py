"""
Publisher is the process that declares available remote functions
Make sure you started this process first.
"""
import time
from daffi import Global


PROC_NAME = "White Rabbit"


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    Global(process_name=PROC_NAME, init_controller=True, host="localhost", port=8888).join()


if __name__ == "__main__":
    main()
