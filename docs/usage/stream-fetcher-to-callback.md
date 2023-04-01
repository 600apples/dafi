In the world of Daffi, yield statements hold a unique significance as they pertain to stream processing.
Streams can be classified into two types: those that go from a fetcher to a callback and those that go from a callback to a fetcher.
Similar to return statements, yield statements can be followed by a tuple or a special "Args" class to send multiple arguments to a remote location.

Streams can also be utilized like events to wait for specific conditions on a remote.


=== "class based approach"

    `stream_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class StreamerService(Callback):
    
        def __post_init__(self):
            self.items = []
    
        def stream_to_service(self, item, process_name):
            self.items.append(item)
            print(f"Received item: {item} from process: {process_name}")
    
        def get_items(self):
            return self.items
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    
    ```
    
    `stream_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Fetcher, Args
    
    logging.basicConfig(level=logging.INFO)
    
    PROCESS_NAME = "streamer client"
    
    
    class StreamerClient(Fetcher):
    
        def __post_init__(self):
            self.items = range(1000)
    
        def stream_to_service(self):
            """Process stream"""
            for item in self.items:
                yield Args(item=item, process_name=PROCESS_NAME)
    
        def get_items(self):
            """Get all items from service"""
            pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888, process_name=PROCESS_NAME)
    
        stream_client = StreamerClient()
        stream_client.stream_to_service()
    
        # get all items from service after stream processing
        items = stream_client.get_items()
        print(items)
    
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
    
    items = []
    
    
    @callback
    def stream_to_service(item, process_name):
        items.append(item)
        print(f"Received item: {item} from process: {process_name}")
    
    
    @callback
    def get_items():
        return items
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `stream_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import fetcher
    from daffi.registry import Args
    
    logging.basicConfig(level=logging.INFO)
    
    PROCESS_NAME = "streamer client"
    
    items = range(1000)
    
    
    @fetcher
    def stream_to_service():
        """Process stream"""
        for item in items:
            yield Args(item=item, process_name=PROCESS_NAME)
    
    
    @fetcher
    def get_items(self):
        """Get all items from service"""
        pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888, process_name=PROCESS_NAME)
    
        stream_to_service()
    
        result = get_items()
        print(result)
    
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
    
