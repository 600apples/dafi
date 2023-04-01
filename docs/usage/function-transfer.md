Even if a particular callback is not present, it is still feasible to transfer and execute a function on the remote side.
In order to utilize this feature, processes need to have static names.


`executor.py` content:
```python
import logging
from daffi import Global

logging.basicConfig(level=logging.INFO)

PROCESS_NAME = "remote executor"


if __name__ == "__main__":
    Global(process_name=PROCESS_NAME, init_controller=True, host="localhost", port=8888).join()
```

`client.py` content:
```python
import logging
from daffi import Global

logging.basicConfig(level=logging.INFO)


async def func_to_transfer():
    """
    Return pid id of remote process.
    If it's uncertain whether an import exists on the remote side,
    it's preferable to explicitly import the used libraries.
    """
    import os
    return os.getpid()


if __name__ == "__main__":
    remote_proc = "remote executor"
    g = Global(host="localhost", port=8888)

    remote_pid = g.transfer_and_call(remote_process=remote_proc, func=func_to_transfer)
    print(f"Pid of remote process: {remote_pid}")
```

Execute in two separate terminals:
```bash
python3 executor.py
python3 client.py
```

!!! note 
    To use UNIX socket instead of TCP for communication, you should remove the `host` and `port` parameters from 
    the initialization of the Global object, and optionally include the `unix_sock_path` parameter.
 