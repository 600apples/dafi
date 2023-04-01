Daffi is intended for two-way communication, with each process capable of having multiple fetchers and callbacks.
There are no explicitly defined servers or clients. By taking advantage of this flexibility,
users can establish intricate relationships between numerous microservices within a system.


`shopping_service.py` content:
```python
import logging
from daffi import Global
from daffi.registry import Callback, Fetcher

logging.basicConfig(level=logging.INFO)

PROCESS_NAME = "shopping service"


class ShoppingService(Callback):

    def __post_init__(self):
        self.shop_items = ["orange", "bread", "cheese"]

    def get_shop_items(self):
        return self.shop_items


class CalculatorClient(Fetcher):

    def calculate_sum(self, *numbers):
        """Proxy fetcher"""
        pass


if __name__ == '__main__':
    g = Global(init_controller=True, host="localhost", port=8888, process_name=PROCESS_NAME)
    # Wait counterpart process
    g.wait_process("calculator service")

    calc_client = CalculatorClient()
    _sum = calc_client.calculate_sum(1, 2, 3)
    print(f"Calculated sum: {_sum}")

    g.join()
```

`calculator_service.py` content:
```python
import logging
from daffi import Global
from daffi.registry import Callback, Fetcher

logging.basicConfig(level=logging.INFO)

PROCESS_NAME = "calculator service"


class CalculatorService(Callback):

    def calculate_sum(self, *numbers):
        return sum(numbers)


class ShoppingClient(Fetcher):

    def get_shop_items(self):
        """Proxy fetcher"""
        pass


if __name__ == '__main__':
    g = Global(host="localhost", port=8888, process_name=PROCESS_NAME)
    # Wait counterpart process
    g.wait_process("shopping service")

    shopping_client = ShoppingClient()
    items = shopping_client.get_shop_items()
    print(f"Received shopping items: {items}")

    g.join()
```

Execute in two separate terminals:
```bash
python3 shopping_service.py
python3 calculator_service.py
```

!!! note 
    To use UNIX socket instead of TCP for communication, you should remove the `host` and `port` parameters from 
    the initialization of the Global object, and optionally include the `unix_sock_path` parameter.
