Example provides a basic and fundamental understanding of client-server communication

=== "class based approach"

    `calculator_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class CalculatorService(Callback):
    
        def calculate_sum(self, *numbers):
            return sum(numbers)
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `calculator_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    class CalculatorClient(Fetcher):
    
        def calculate_sum(self, *numbers):
            """
            Note: functions without a body are treated as proxies for remote callbacks.
            All arguments provided to this function will be sent to the remote service as-is.
            """
            pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        calc_client = CalculatorClient()
        result = calc_client.calculate_sum(1, 2)
        print(result)
    
        result = calc_client.calculate_sum(10, 20, 30)
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
    def calculate_sum(*numbers):
        return sum(numbers)
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `calculator_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    @fetcher
    def calculate_sum(*numbers):
        """
        Note: functions without a body are treated as proxies for remote callbacks.
        All arguments provided to this function will be sent to the remote service as-is.
        """
        pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        result = calculate_sum(1, 2)
        print(result)
    
        result = calculate_sum(10, 20, 30)
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
    
