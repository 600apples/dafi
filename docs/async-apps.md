Most of daffi methods works fine with both sync and async applications but some of them blocks event loop. 

Blocking methods have async conterparts which should be used for asynchronous processes.

For example [Global](code-reference/global.md) object has method `join` which is blocking method and cannot be used for applications 
which leverage on event loop. 

But `Global` also has `join_async` method which fits async non blocking model.

Also there are two methods `get` and `get_async` to take result depending on application type.

As example we can use `BG` execution modifier (run in background) which returns instance of `AsyncResult` instead of result itself:


```python
from daffi import fetcher, BG

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
from daffi import callback

@callback
def trigger_me():
    print("Triggered")
```

In `process-2` we can declare sync or async fetcher to trigger callback:

process-2
```python
from daffi import fetcher, FG

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
from daffi import fetcher, FG

@fetcher(FG)
async def trigger_me():
    pass

async def main():
    await trigger_me()

# ....
```
