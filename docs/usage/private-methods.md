Both callbacks and fetchers can contain private methods that do not behave as remote callbacks
or serve as pointers on remote callbacks (in the case of fetchers). To create such methods, users have two options:


1. They can add a leading underscore to the method name.
2. They can mark the method as local using the `local` decorator.


`calculator_service.py` content:
```python
import logging
from daffi import Global
from daffi.registry import Callback
from daffi.decorators import local

logging.basicConfig(level=logging.INFO)


class CalculatorService(Callback):

    def calculate_sum(self, num1, num2):
        return self.get_sum(num1, num2)

    @local
    def get_sum(self, num1, num2):
        return num1 + num2


if __name__ == '__main__':
    Global(init_controller=True, host="localhost", port=8888).join()
```

`calculator_client.py` content:
```python
import logging
from daffi import Global
from daffi.registry import Fetcher, Args
from daffi.decorators import local

logging.basicConfig(level=logging.INFO)


class CalculatorClient(Fetcher):

    def calculate_sum(self, num1, num2):
        return self.create_args(num1, num2)

    @local
    def create_args(self, num1, num2):
        return Args(num1, num2)


if __name__ == '__main__':
    g = Global(host="localhost", port=8888)

    calc_client = CalculatorClient()
    result = calc_client.calculate_sum(1, 2)
    print(result)

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
    
