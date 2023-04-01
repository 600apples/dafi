A process can be given a meaningful name by the user when initializing the Global object with the `process_name` argument.
By default, the name is generated automatically.
Naming processes can be useful in situations where one process needs to wait for another to start.


`calculator_service.py` content:
```python
import logging
from daffi import Global

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    g = Global(init_controller=True, host="localhost", port=8888, process_name="calculator service")
    g.wait_process(process_name="calculator client")

    print("Calculator client has been started...")

    g.join()
```

`calculator_client.py` content:
```python
import time
import logging
from daffi import Global

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    g = Global(host="localhost", port=8888, process_name="calculator client")

    time.sleep(5)

    g.stop()
```

Execute in two separate terminals:
```bash
python3 calculator_service.py
python3 calculator_client.py
```

!!! note 
    To use UNIX socket instead of TCP for communication, you should remove the `host` and `port` parameters from 
    the initialization of the Global object, and optionally include the `unix_sock_path` parameter.
    