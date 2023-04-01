The Callback class allows users to include extra initialization arguments.
By default, any subclass of Callback is automatically initialized without any additional input.
To turn off this default behavior, users can set the `auto_init` flag to False.

In this scenario, it is the user's responsibility to explicitly create an instance of the class.


`calculator_service.py` content:
```python
import logging
from daffi import Global
from daffi.registry import Callback

logging.basicConfig(level=logging.INFO)


class CalculatorService(Callback):

    auto_init = False

    def __init__(self, multiplier):
        super().__init__()
        self.multiplier = multiplier

    def calculate_sum(self, num1, num2):
        num1 *= self.multiplier
        num2 *= self.multiplier
        return num1 + num2


if __name__ == '__main__':
    calc_service = CalculatorService(multiplier=3)
    Global(init_controller=True, host="localhost", port=8888).join()
```

`calculator_client.py` content:
```python
import logging
from daffi import Global
from daffi.registry import Fetcher

logging.basicConfig(level=logging.INFO)


class CalculatorClient(Fetcher):

    def calculate_sum(self, num1, num2):
        pass

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
    
