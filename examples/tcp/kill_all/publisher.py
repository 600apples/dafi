"""
Publisher is the process that declares available remote functions
"""
import logging
import time
from daffi import Global

logging.basicConfig(level=logging.INFO)


def main():
    # For this example consumer should work
    g = Global(init_controller=True, host="localhost", port=8888)
    time.sleep(10)
    g.kill_all()

    print("Killed!")
    time.sleep(5)
    g.stop()


if __name__ == "__main__":
    main()
