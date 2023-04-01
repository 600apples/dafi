Just like how streams can be initiated from a fetcher to a callback, callbacks can also initialize streams using yield statements.
In this scenario, the fetcher receives a generator as the result.

Streams can also be utilized like events to wait for specific conditions on a remote.


=== "class based approach"

    `stream_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class StreamerService(Callback):
    
        def generate_stream(self, end):
            for i in range(end):
                yield i
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `stream_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    class StreamerClient(Fetcher):
    
        def generate_stream(self, end):
            """Generate stream by callback"""
            pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        stream_client = StreamerClient()
        result = stream_client.generate_stream(end=1000)
    
        for item in result:
            print(item)
    
        g.stop()
    ```
    
    Execute in two separate terminals:
    ```bash
    python3 stream_service.py
    python3 stream_client.py
    ```

=== "decorator based approach"
    
    `stream_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    @callback
    def generate_stream(end):
        for item in range(end):
            yield item
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `stream_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    @fetcher
    def generate_stream(end):
        pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        result = generate_stream(1000)
        for item in result:
            print(item)
    
        g.stop()
    ```
    
    Execute in two separate terminals:
    ```bash
    python3 stream_service.py
    python3 stream_client.py
    ```


!!! note 
    To use UNIX socket instead of TCP for communication, you should remove the `host` and `port` parameters from 
    the initialization of the Global object, and optionally include the `unix_sock_path` parameter.
    
