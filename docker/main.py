import os
from daffi import Global, GlobalContextError


if __name__ == "__main__":

    dafi_process_name = "daffi-controller"
    if not (dafi_port := os.getenv("DAFI_PORT")):
        raise GlobalContextError("'DAFI_PORT' environment variable not found!")

    g = Global(
        process_name=dafi_process_name,
        host="localhost",
        port=dafi_port,
        init_controller=True,
        init_node=False,
    ).join()
