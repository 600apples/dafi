You can register callbacks with the same name on multiple processes, allowing for simultaneous execution.
In addition to one-to-one communication, there is a specific execution modifier called `BROADCAST` that can be utilized in fetchers.
The `BROADCAST` modifier ensures that the result is only returned after all callbacks have completed their work.
 
The resulting output takes on a dictionary structure, with keys representing the process names and corresponding values representing the computed results.


```python
{
    "process-1":  "<result-1>",
    "process-2":  "<result-2>",
    "process-N":  "<result-N>"
}
```

!!! warning 
    Make sure you initialized only one process with `init_controller=True` argument


=== "class based approach"

    `burger_menu_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class BurgerMenu(Callback):
    
        def get_menu(self):
            return ["The IceBurg", "The Grill Thrill", "Burger Mania", "Chicha Burger"]
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888, process_name="burger menu").join()
    ```
    
    `hotdog_menu_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class HotDogMenu(Callback):
    
        def get_menu(self):
            return ["Wiener", "Weenie", "Coney", "Red Hot"]
    
    
    if __name__ == '__main__':
        Global(host="localhost", port=8888, process_name="hotdog menu").join()
    ```
    
    `menu_client.py` content:
    ```python
    import logging
    from daffi import Global, BROADCAST
    from daffi.registry import Fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    class MenuFetcher(Fetcher):
        exec_modifier = BROADCAST
    
        def get_menu(self):
            pass
    
    
    if __name__ == '__main__':
        menu_fetcher = MenuFetcher()
        g = Global(host="localhost", port=8888)
    
        # Make sure all processes started
        for proc in ("burger menu", "hotdog menu"):
            g.wait_process(proc)
    
        menus = menu_fetcher.get_menu()
        print(menus)
        # {'burger menu': ['The IceBurg', 'The Grill Thrill', 'Burger Mania', 'Chicha Burger'], 'hotdog menu': ['Wiener', 'Weenie', 'Coney', 'Red Hot']}
    
        g.stop()
    ```
    
    Execute in three separate terminals:
    ```bash
    python3 burger_menu_service.py
    python3 hotdog_menu_service.py
    python3 menu_client.py
    ```

=== "decorator based approach"
    
    `burger_menu_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    @callback
    def get_menu():
        return ["The IceBurg", "The Grill Thrill", "Burger Mania", "Chicha Burger"]
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888, process_name="burger menu").join()
    
    ```
    
    `hotdog_menu_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    @callback
    def get_menu():
        return ["Wiener", "Weenie", "Coney", "Red Hot"]
    
    
    if __name__ == '__main__':
        Global(host="localhost", port=8888, process_name="hotdog menu").join()
    ```
    
    `menu_client.py` content:
    ```python
    import logging
    from daffi import Global, BROADCAST
    from daffi.decorators import fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    @fetcher(exec_modifier=BROADCAST)
    def get_menu():
        pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        # Make sure all processes started
        for proc in ("burger menu", "hotdog menu"):
            g.wait_process(proc)
    
        menus = get_menu()
        print(menus)
        # {'burger menu': ['The IceBurg', 'The Grill Thrill', 'Burger Mania', 'Chicha Burger'], 'hotdog menu': ['Wiener', 'Weenie', 'Coney', 'Red Hot']}
        
        g.stop()
    ```
    
    Execute in three separate terminals:
    ```bash
    python3 burger_menu_service.py
    python3 hotdog_menu_service.py
    python3 menu_client.py
    ```


!!! note 
    To use UNIX socket instead of TCP for communication, you should remove the `host` and `port` parameters from 
    the initialization of the Global object, and optionally include the `unix_sock_path` parameter.
