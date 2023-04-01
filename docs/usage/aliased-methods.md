Both callbacks and fetchers can contain aliased methods, which means that the methods appear under different names on the remote side.
This is particularly useful for fetchers, as it allows multiple fetchers with different names and internal logic to point to a single callback.


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
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `calculator_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Fetcher, Args
    from daffi.decorators import alias
    
    logging.basicConfig(level=logging.INFO)
    
    
    class CalculatorClient(Fetcher):
    
        def __post_init__(self):
            self.multiplier = 2
    
        def calculate_sum(self, num1, num2):
            """Default proxy behavior"""
            pass
    
        @alias("calculate_sum")
        def calculate_sum_with_multiplier(self, num1, num2):
            """Alias to the same `calculate_sum` callback but with different internal logic"""
            num1 *= self.multiplier
            num2 *= self.multiplier
            return Args(num1, num2)
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        calc_client = CalculatorClient()
        result = calc_client.calculate_sum(1, 2)
        print(result)
    
        result = calc_client.calculate_sum_with_multiplier(1, 2)
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
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `calculator_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import fetcher, alias
    from daffi.registry import Args
    
    logging.basicConfig(level=logging.INFO)
    
    multiplier = 2
    
    
    @fetcher
    def calculate_sum(num1, num2):
        """Default proxy behavior"""
        pass
    
    
    @fetcher
    @alias("calculate_sum")
    def calculate_sum_with_multiplier(num1, num2):
        """Alias to the same `calculate_sum` callback but with different internal logic"""
        num1 *= multiplier
        num2 *= multiplier
        return Args(num1, num2)
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        result = calculate_sum(1, 2)
        print(result)
    
        result = calculate_sum_with_multiplier(1, 2)
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
    
