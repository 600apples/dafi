Fetcher functions exhibit different behavior based on whether they have a body or not.
If a fetcher function does not have a body, it acts as a proxy, transmitting all provided arguments directly
to the remote callback without any modification. On the other hand, if a fetcher function has a body,
only the arguments returned from the function will be transmitted to the remote callback.
This allows for additional intermediate logic to be executed before sending the arguments to the remote location.

Arguments can be returned as tuple or using special `Args` class.


=== "class based approach"

    `calculator_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class CalculatorService(Callback):
    
        def calculate_sum(self, num1, num2):
            return num1 + num2
    
        def calculate_difference(self, num1, num2):
            return num1 - num2
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `calculator_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Fetcher, Args
    
    logging.basicConfig(level=logging.INFO)
    
    
    class CalculatorClient(Fetcher):
    
        def __post_init__(self):
            self.multiplier = 2
    
        def calculate_sum(self, num1, num2):
            """Return arguments as tuple"""
            num1 *= self.multiplier
            num2 *= self.multiplier
            return num1, num2
    
    
        def calculate_difference(self, num1, num2):
            """Return arguments as `Args` class"""
            num1 *= self.multiplier
            num2 *= self.multiplier
            return Args(num1=num1, num2=num2)
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        calc_client = CalculatorClient()
        result = calc_client.calculate_sum(1, 2)
        print(result)
    
        result = calc_client.calculate_difference(50, 20)
        print(result)
    
        g.stop()
    ```
    
    Execute in two separate terminals:
    ```bash
    python3 calculator_service.py
    python3 calculator_client.py
    ```

=== "decorator based approach"

    `calculator_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    @callback
    def calculate_sum(num1, num2):
        return num1 + num2
    
    
    @callback
    def calculate_difference(num1, num2):
        return num1 - num2
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `calculator_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import fetcher
    from daffi.registry import Args
    
    logging.basicConfig(level=logging.INFO)
    
    multiplier = 2
    
    
    @fetcher
    def calculate_sum(num1, num2):
        """Return arguments as tuple"""
        num1 *= multiplier
        num2 *= multiplier
        return num1, num2
    
    
    @fetcher
    def calculate_difference(num1, num2):
        """Return arguments as `Args` class"""
        num1 *= multiplier
        num2 *= multiplier
        return Args(num1=num1, num2=num2)
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        result = calculate_sum(1, 2)
        print(result)
    
        result = calculate_difference(50, 20)
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
    
