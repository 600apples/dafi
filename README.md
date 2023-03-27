<h1> 
    <img src="https://600apples.github.io/dafi/images/logo.png"
    width="40"
    height="40"
    style="float: left;">
    Daffi
</h1>


![test and validate](https://github.com/600apples/dafi/actions/workflows/test_and_validate.yml/badge.svg)
![publish docs](https://github.com/600apples/dafi/actions/workflows/publish_docs.yml/badge.svg)
![coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/600apples/c64b2cee548575858e40834754432018/raw/covbadge.json)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Linux](https://svgshare.com/i/Zhy.svg)](https://svgshare.com/i/Zhy.svg)
[![macOS](https://svgshare.com/i/ZjP.svg)](https://svgshare.com/i/ZjP.svg)
[![Downloads](https://static.pepy.tech/badge/daffi/month)](https://pepy.tech/project/daffi)

Daffi facilitates remote computing and enables remote procedure calls between multiple endpoints.
It supports many-to-many relationships between endpoints, allowing for seamless communication between distributed systems.
The library abstracts the complexities of remote computing and provides a user-friendly interface for initiating and managing remote procedure calls.
It also offers various features such as fault tolerance, load balancing, streaming and security, to ensure reliable and secure communication between endpoints.

## Documentation

View full documentation at: [https://600apples.github.io/dafi/](https://600apples.github.io/dafi/)


## Basic example

You need to create two files `shopping_service.py` and `shopper.py`

`shopping_service.py` - represents a set of remote callbacks that can be triggered by a client application.

`shopper.py` - represents shopping_service client

#### Class based approach


`shopping_service.py`:
```python
import logging
from daffi import Global
from daffi.registry import Callback

logging.basicConfig(level=logging.INFO)


class ShoppingService(Callback):

    def __post_init__(self):
        self.shopping_list = []

    def get_items(self):
        """Return all items that are currently present in shopping list"""
        return self.shopping_list

    def add_item(self, item):
        """Add new item to shopping list"""
        self.shopping_list.append(item)

    def clear_items(self):
        """Clear shopping list"""
        self.shopping_list.clear()


if __name__ == '__main__':
    Global(init_controller=True, host="localhost", port=8888).join()
```
(This script is complete, it should run "as is")


`shopper.py`:
```python
import logging
from daffi import Global
from daffi.decorators import alias
from daffi.registry import Fetcher

logging.basicConfig(level=logging.INFO)


class Shopper(Fetcher):
    """
    Note: Functions without a body are treated as proxies for remote callbacks.
    All arguments provided to this function will be sent to the remote service as-is.
    """

    def get_items(self):
        """Return all items that are currently present in shopping list."""
        pass

    def add_item(self, item):
        """Add new item to shopping list."""
        pass

    def clear_items(self):
        """Clear shopping list"""
        pass

    @alias("add_item")
    def add_many_items(self, *items):
        """
        Alias for `add_item` callback.
        This function shows streaming capabilities for transferring data from one service to another.
        """
        for item in items:
            yield item


if __name__ == '__main__':
    g = Global(host="localhost", port=8888)

    shopper = Shopper()
    items = shopper.get_items()
    print(items)

    shopper.add_item("orange")
    items = shopper.get_items()
    print(items)

    shopper.add_many_items("bread", "cheese")
    items = shopper.get_items()
    print(items)

    shopper.clear_items()
    items = shopper.get_items()
    print(items)

    g.stop()
```
(This script is complete, it should run "as is")

To check the full example, you need to execute two scripts in separate terminals

```bash
python3 shopping_service.py

...
INFO 2023-03-27 19:49:45 | controller[0x91adb83e] | Controller has been started successfully. Process identificator: '0x91adb83e'. Connection info: tcp: [ host '[::]', port: 8888 ]
INFO 2023-03-27 19:49:45 | node[0x91adb83e] | Node has been started successfully. Process identificator: '0x91adb83e'. Connection info: tcp: [ host '[::]', port: 8888 ]
```

```bash
python3 shopper.py

...
INFO 2023-03-27 19:53:15 | node[0xd7e5d488] | Node has been started successfully. Process identificator: '0xd7e5d488'. Connection info: tcp: [ host '[::]', port: 8888 ]
[]
['orange']
['orange', 'bread', 'cheese']
[]
INFO 2023-03-27 19:53:15 | node[0xd7e5d488] | Node stopped.
```

### Decorators base approach

`shopping_service.py`:
```python
import logging
from daffi import Global
from daffi.decorators import callback

logging.basicConfig(level=logging.INFO)

shopping_list = []


@callback
def get_items():
    """Return all items that are currently present in shopping list"""
    return shopping_list


@callback
def add_item(item):
    """Add new item to shopping list"""
    shopping_list.append(item)


@callback
def clear_items():
    """Clear shopping list"""
    shopping_list.clear()


if __name__ == '__main__':
    Global(init_controller=True, host="localhost", port=8888).join()
```
(This script is complete, it should run "as is")


`shopper.py`:
```python
"""
Note: Functions without a body are treated as proxies for remote callbacks.
    All arguments provided to this function will be sent to the remote service as-is.
"""
import logging
from daffi import Global
from daffi.decorators import alias, fetcher

logging.basicConfig(level=logging.INFO)


@fetcher
def get_items():
    """Return all items that are currently present in shopping list."""
    pass


@fetcher
def add_item(item):
    """Add new item to shopping list."""
    pass


@fetcher
def clear_items():
    """Add new item to shopping list."""
    pass


@alias("add_item")
@fetcher
def add_many_items(*items):
    """
    Alias for `add_item` callback.
    This function shows streaming capabilities for transferring data from one service to another.
    """
    for item in items:
        yield item


if __name__ == '__main__':
    g = Global(host="localhost", port=8888)

    items = get_items()
    print(items)

    add_item("orange")
    items = get_items()
    print(items)

    add_many_items("bread", "cheese")
    items = get_items()
    print(items)

    clear_items()
    items = get_items()
    print(items)

    g.stop()
```
(This script is complete, it should run "as is")

To check the full example, you need to execute two scripts in separate terminals

```bash
python3 shopping_service.py

...
INFO 2023-03-27 20:31:27 | controller[0xbac16ef4] | Controller has been started successfully. Process identificator: '0xbac16ef4'. Connection info: tcp: [ host '[::]', port: 8888 ]
INFO 2023-03-27 20:31:27 | node[0xbac16ef4] | Node has been started successfully. Process identificator: '0xbac16ef4'. Connection info: tcp: [ host '[::]', port: 8888 ]
```

```bash
python3 shopper.py

...
INFO 2023-03-27 20:31:43 | node[0xb9e10444] | Node has been started successfully. Process identificator: '0xb9e10444'. Connection info: tcp: [ host '[::]', port: 8888 ]
[]
['orange']
['orange', 'bread', 'cheese']
[]
INFO 2023-03-27 20:31:44 | node[0xb9e10444] | Node stopped.
```
