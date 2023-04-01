Daffi utilizes classes known as [execution modifiers](../execution-modifiers.md) to transmit requests to a remote process.
These modifiers define the manner in which requests should be executed and how the system should await the resulting computation. 
The default modifier, which we have implicitly used in previous examples, is `FG`, which stands for "foreground." 
This modifier blocks the main process until the computed result is returned.

In the following example, we will use the `BG` execution modifier, which is short for "background." 
This modifier returns an instance of `AsyncResult` rather than the result itself, 
making it appropriate for long-running tasks or tasks in which the result is not critical.


=== "class based approach"

    `calculator_service.py`
    content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class CalculatorService(Callback):
    
        def calculate_fibonacci(self, n):
            # Check if n is 0
            # then it will return 0
            if n == 0:
                return 0
    
            # Check if n is 1,2
            # it will return 1
            elif n == 1 or n == 2:
                return 1
    
            else:
                return self.calculate_fibonacci(n - 1) + self.calculate_fibonacci(n - 2)
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `calculator_client.py`
    content:
    ```python
    import time
    import logging
    from daffi import Global, BG, FG
    from daffi.registry import Fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    class CalculatorClient(Fetcher):
        
        # Execution modifier are shared across all fetcher's methods
        exec_modifier = BG(timeout=30)
    
        def calculate_fibonacci(self, n):
            """Proxy fetcher"""
            pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        calc_client = CalculatorClient()
        feature = calc_client.calculate_fibonacci(20)
    
        print("Waiting")
        time.sleep(10)
    
        result = feature.get()
        print(f"Fibonacci result: {result}")
    
        # Execution modifier can be specified at execution time
        result = calc_client.calculate_fibonacci.call(exec_modifier=FG, n=15)
        print(f"Fibonacci result: {result}")
    
        g.stop()
    ```
    
    Execute in two separate terminals:
    ```bash
    python3 calculator_service.py
    python3 calculator_client.py
    ```

=== "decorator based approach"

    `calculator_service.py`
    content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    @callback
    def calculate_fibonacci(n):
        # Check if n is 0
        # then it will return 0
        if n == 0:
            return 0
    
        # Check if n is 1,2
        # it will return 1
        elif n == 1 or n == 2:
            return 1
    
        else:
            return calculate_fibonacci(n - 1) + calculate_fibonacci(n - 2)
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `calculator_client.py`
    content:
    ```python
    import time
    import logging
    from daffi import Global, BG, FG
    from daffi.decorators import fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    @fetcher(exec_modifier=BG(timeout=30))
    def calculate_fibonacci(n):
        """Proxy fetcher"""
        pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        feature = calculate_fibonacci(20)
    
        print("Waiting")
        time.sleep(10)
    
        result = feature.get()
        print(f"Fibonacci result: {result}")
    
        # Execution modifier can be specified at execution time
        result = calculate_fibonacci.call(exec_modifier=FG, n=15)
        print(f"Fibonacci result: {result}")
    
        g.stop()
    ```
    
    Execute in two
    separate
    terminals:
    ```bash
    python3 calculator_service.py
    python3 calculator_client.py
    ```

!!! note 
    To use UNIX socket instead of TCP for communication, you should remove the `host` and `port` parameters from 
    the initialization of the Global object, and optionally include the `unix_sock_path` parameter.



