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
[![Downloads](https://static.pepy.tech/badge/daffi/month)](https://pepy.tech/project/daffi)

Daffi facilitates remote computing and enables remote procedure calls between multiple endpoints.
It supports many-to-many relationships between endpoints, allowing for seamless communication between distributed systems.
The library abstracts the complexities of remote computing and provides a user-friendly interface for initiating and managing remote procedure calls.
It also offers various features such as fault tolerance, load balancing, streaming and security, to ensure reliable and secure communication between endpoints.

Daffi comprises three primary classes:

- *Global* - Initialization entrypoint. Once *Global* object is initialized application can respond on remote requests and trigger remote callbacks itself.
- *Callback* - Represents a collection of methods encapsulated in a class inherited from *Callback* or a standalone function decorated with the *callback* decorator. These functions/methods can be triggered from another process.
- *Fetcher* - Represents a collection of methods encapsulated in a class inherited from *Fetcher* or a standalone function decorated with the *fetcher* decorator. These functions/methods serve as triggers for the corresponding callbacks defined in another process.

## Basic example

You need to create two files `shopping_service.py` and `shopper.py`

`shopping_service.py` - represents a set of remote callbacks that can be triggered by a client application.

`shopper.py` - represents shopping_service client (fetcher)


=== "class based approach"

    `shopping_service.py`:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class ShoppingService(Callback):
        auto_init = True # class is automatically initialized, eliminating the need to manually create an object.
    
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
    from daffi.registry import Fetcher, FetcherMethod
    
    logging.basicConfig(level=logging.INFO)
    
    
    class Shopper(Fetcher):
        # Ensure that the names of the fetcher methods align with the names of the ShoppingService being exposed.
        get_items = FetcherMethod()
        add_item = FetcherMethod()
        clear_items = FetcherMethod()
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)

        shopper = Shopper()
        
        print("current shoping list items:")
        items = shopper.get_items()
        print(items)
        
        print("adding 'orange' to shopping list and getting updated list from remote")
        shopper.add_item("orange")
        items = shopper.get_items()
        print(items)
        
        print("adding 'milk' to shopping list and getting updated list from remote")
        shopper.add_item("milk")
        items = shopper.get_items()
        print(items)
        
        print("cleaning shopping list and geting updated list from remote")
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
    INFO:    2023-06-18 22:40:34 | controller[0x24d2ab36] | Controller has been started successfully. Process identificator: '0x24d2ab36'. Connection info: tcp: [ host '[::]', port: 8888 ]
    INFO:    2023-06-18 22:40:34 | node[0x24d2ab36] | Node has been started successfully. Process identificator: '0x24d2ab36'. Connection info: tcp: [ host '[::]', port: 8888 ]
    INFO:    2023-06-18 22:40:34 | node[0x24d2ab36] | Node '0x24d2ab36' has been connected to Controller
    ```
    
    ```bash
    python3 shopper.py
    
    ...
    INFO:    2023-06-18 22:46:14 | node[0xe1f8d8a3] | Node has been started successfully. Process identificator: '0xe1f8d8a3'. Connection info: tcp: [ host '[::]', port: 8888 ]
    INFO:    2023-06-18 22:46:14 | node[0xe1f8d8a3] | Node '0xe1f8d8a3' has been connected to Controller
    current shoping list items:
    []
    adding 'orange' to shopping list and getting updated list from remote
    ['orange']
    adding 'milk' to shopping list and getting updated list from remote
    ['orange', 'milk']
    cleaning shopping list and geting updated list from remote
    []
    INFO:    2023-06-18 22:46:14 | node[0xe1f8d8a3] | Node stopped.
    ```

=== "decorator based approach"
    
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
    from daffi.decorators import fetcher
    
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
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
        
        print("current shoping list items:")
        items = get_items()
        print(items)
        
        print("adding 'orange' to shopping list and getting updated list from remote")
        add_item("orange")
        items = get_items()
        print(items)
        
        print("adding 'milk' to shopping list and getting updated list from remote")
        add_item("milk")
        items = get_items()
        print(items)
        
        print("cleaning shopping list and geting updated list from remote")
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
    INFO:    2023-06-18 22:52:06 | node[0xabe27200] | Node has been started successfully. Process identificator: '0xabe27200'. Connection info: tcp: [ host '[::]', port: 8888 ]
    current shoping list items:
    INFO:    2023-06-18 22:52:06 | node[0xabe27200] | Node '0xabe27200' has been connected to Controller
    []
    adding 'orange' to shopping list and getting updated list from remote
    ['orange']
    adding 'milk' to shopping list and getting updated list from remote
    ['orange', 'milk']
    cleaning shopping list and geting updated list from remote
    []
    INFO:    2023-06-18 22:52:07 | node[0xabe27200] | Node stopped.
    ```


!!! note 
    To use UNIX socket instead of TCP for communication, you should remove the `host` and `port` parameters from 
    the initialization of the Global object, and optionally include the `unix_sock_path` parameter.
