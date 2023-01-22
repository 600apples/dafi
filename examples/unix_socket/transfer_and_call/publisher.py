"""
Publisher is the process that declares available remote functions
Make sure you started this process first.
"""
import time
import logging
from daffi import Global

logging.basicConfig(level=logging.INFO)

PROC_NAME = "White Rabbit"


def main():
    # Process name is not required argument and will be generated automatically if not provided.
    g = Global(process_name=PROC_NAME, init_controller=True)
    time.sleep(120)
    print("Exit.")
    g.stop()


if __name__ == "__main__":
    main()
