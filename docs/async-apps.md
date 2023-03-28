The majority of Daffi's methods are compatible with both synchronous and asynchronous applications, but certain methods may impede the event loop. 
In such cases, it is recommended to use the asynchronous counterparts of these blocking methods. 

For instance, the Global object's `join` method is a blocking method that cannot be used with event loop-dependent applications. 
However, the `join_async` method is an asynchronous, non-blocking alternative. 
There are two methods available for retrieving results: `get` and `get_async`, depending on the application type.

As example we can use `BG` execution modifier (run in background) which returns instance of `AsyncResult` instead of result itself:


```python
from daffi import BG
from daffi.decorators import fetcher

@fetcher(BG)
def my_callback():
    pass

future = my_callback()

# get result in blocking manner
result = future.get()

# get result in async applications
result = await future.get_async()
```

Despite on remote callback type we can declare synchronous or asynchronous fetcher depends on our application.

For instance if in `process-1` we have callback with name `trigger_me`:

process-1
```python
from daffi.decorators import callback

@callback
def trigger_me():
    print("Triggered")
```

In `process-2` we can declare sync or async fetcher to trigger callback:

process-2
```python
from daffi import FG
from daffi.decorators import fetcher

@fetcher(FG)
def trigger_me():
    pass

def main():
    trigger_me()

# .... 
```

The more appropriate syntax for asynchronous applications will be:

process-2
```python
from daffi import FG
from daffi.decorators import fetcher

@fetcher(FG)
async def trigger_me():
    pass

async def main():
    await trigger_me()

# ....
```
