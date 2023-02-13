In previous [example](./basic-example.md) we considered only `consumer` to `publisher` communication. In other words only `consumer` triggered remote callbacks. 

Lets modify `publisher.py` and `consumer.py` so that each of these processes registers a callback and triggers a callback from other process.

For example lets keep `sum_of_two_numbers` callback in `publisher.py` but create new function `consumer_time` in `consumer.py`.
 `consumer_time` should just return current UTC timestamp of consumer process.
 
 
##### publisher.py

```python
# publisher.py
import asyncio
import logging
from daffi import Global, callback, fetcher, __body_unknown__, FG

logging.basicConfig(level=logging.INFO)


@callback  # (1)
def sum_of_two_numbers(a: int, b: int) -> int:
    return a + b


@fetcher(FG)
async def consumer_time() -> float:
    __body_unknown__()


async def runner(): # (2)
    for _ in range(10):
        current_consumer_time = consumer_time()
        print(f"Current consumer time: {current_consumer_time}")

        await asyncio.sleep(3)


if __name__ == '__main__':
    g = Global(host="localhost", port=8888, init_controller=True, process_name="publisher")  # (3)

    g.wait_process("consumer") # (4)

    asyncio.run(runner()) # (5)

    g.stop()
```
(This script is complete, it should run "as is")


- (1) Here we registered `sum_of_two_numbers` function as remote callback. Syntax is the same as in previous examples. 
- (2) `runner` is async function that will be running in asyncio event loop. This function triggers `consumer_time` remote callback that is registered in `consumer.py` process. Execution happen to be 10 times in the cycle.
- (3) Here we initialize `Global` object. In additional to provided host and port we also assign specific process name. This way we can wait process each other by name.
- (4) Here we wait for `consumer` process to be started. 
- (5) Start main `runner` function here


##### consumer.py

```python
# consumer.py
import asyncio
import logging
from datetime import datetime
from daffi import Global, FG, fetcher, __body_unknown__, callback

logging.basicConfig(level=logging.INFO)


@callback # (1)
async def consumer_time() -> float:
    return datetime.utcnow().timestamp()


@fetcher(FG)  # (2)
async def sum_of_two_numbers(a: int, b: int) -> int:
    __body_unknown__(a, b)


async def runner():  # (3)
    for _ in range(10):
        result = sum_of_two_numbers(5, 15)
        print(f"Result = {result}")

        await asyncio.sleep(3)


if __name__ == "__main__":

    g = Global(host="localhost", port=8888, process_name="consumer")  # (4)

    g.wait_process("publisher")  # (5)

    asyncio.run(runner())  # (6)

    g.stop()
```
(This script is complete, it should run "as is")


- (1) Here we registered `consumer_time` function as remote callback. 
- (2) Here we created fetcher for `sum_of_two_numbers` function that is registered as callback in `publisher.py` process. 
- (3) `runner` is async function that will be running in asyncio event loop. This function triggers `sum_of_two_numbers` remote callback that is registered in `publisher.py` process. Execution happen to be 10 times in the cycle.
- (4) Here we initialize `Global` object. In additional to provided host and port we also assign specific process name. This way we can wait process each other by name.
- (5) Here we wait for `publisher` process to be started. 
- (6) Start main `runner` function here

!!! note
    Daffi works fine with both synchronous and asynchronous functions but some methods blocks event loop so daffi has async method options for such situations.
    
    Read more [here](async-apps.md) if your application leverage on event loop. 
    
<hr>

Sometimes you need to register specific callback to make it available to call from remote processes but you also want to call callback with the same name registered on remote (aka celery style).

It is common practice in dockerized applications to have one code base but different entrypoint commands.
For such situations you can register your function as `callback` and `fetcher` at the same time

Example:

```python
import logging
from daffi import fetcher, callback

logging.basicConfig(level=logging.INFO)

@fetcher
@callback
async def my_callback() -> str:
    # Do something
    return "abcdefg"

...
```

There is another decorator `callback_and_fetcher` which you can use if you want your function to be combination of `fetcher` and `callback`

Lets modify `publisher.py` and `consumer.py` from previous example so that both of them have one function `get_process_name` which returns name of process.


##### publisher.py
```python
# publisher.py
import asyncio
import logging
from daffi import Global, callback_and_fetcher, FG

logging.basicConfig(level=logging.INFO)

PROCESS_NAME = "publisher"


@callback_and_fetcher  # (1)
async def get_process_name() -> str:
    return PROCESS_NAME


async def runner():  # (2)
    for _ in range(10):
        remote_process_name = get_process_name() & FG
        print(f"{PROCESS_NAME} called {remote_process_name!r}")

        await asyncio.sleep(3)


if __name__ == "__main__":
    g = Global(host="localhost", port=8888, init_controller=True, process_name=PROCESS_NAME)

    g.wait_process("consumer")

    asyncio.run(runner())

    g.stop()
```

- (1) We registered `get_process_name` function as `fetcher` and `callback`. It means this function is visible to other processes
 by name due to it is `callback` but if we trigger it locally it works as `fetcher` (trigger remote callback with name `get_process_name` on remote)
- (2) We declared runner function which triggers `get_process_name` 10 times in cycle.


##### consumer.py
```python
# consumer.py
import asyncio
import logging
from daffi import Global, callback_and_fetcher, FG

logging.basicConfig(level=logging.INFO)

PROCESS_NAME = "consumer"


@callback_and_fetcher
async def get_process_name() -> str:
    return PROCESS_NAME


async def runner():
    for _ in range(10):
        remote_process_name = get_process_name() & FG
        print(f"{PROCESS_NAME} called {remote_process_name!r}")

        await asyncio.sleep(3)


if __name__ == "__main__":

    g = Global(host="localhost", port=8888, process_name="consumer")

    g.wait_process("publisher")

    asyncio.run(runner())

    g.stop()
```

`consumer.py` does pretty much the same as `publisher.py`. The only difference then `publisher.py` initializes controller and node whereas `consumer.py` only node.

This way you can have only one common script instead of `publisher.py` and `consumer.py`. The only thing you should do is to pass env variables `PROCESS_NAME` and `INIT_CONTROLLER` (True or False) during initialization. 


<hr>
You can check another examples at: [https://github.com/600apples/dafi/tree/main/examples](https://github.com/600apples/dafi/tree/main/examples)

