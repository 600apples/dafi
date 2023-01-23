Let's create simple application.

This application will consist of two python files `publisher.py` and `consumer.py`.

- `publisher.py` will register a simple function `sum_of_two_numbers` as a remote callback. This function should take 2 numbers and return their sum.
- `consumer.py` will call the function `sum_of_two_numbers` remotely passing it 2 numbers as arguments,  wait for the result and print it.

##### publisher.py
```python
# publisher.py
import logging
from daffi import Global, callback

logging.basicConfig(level=logging.INFO)


@callback # (1)!
def sum_of_two_numbers(a: int, b: int) -> int:
    return a + b


if __name__ == '__main__':
    
    g = Global(host="localhost", port=8888, init_controller=True) # (2)!
    g.join() # (3)!
```
(This script is complete, it should run "as is")

- on the line with marker `!(1)` we use `callback` decorator which registers the function as a remote callback.
Describing  `callback` in other words, it makes the function `sum_of_two_numbers` visible to all other processes where daffi is running.
More details about using `callback` decorator you can find [here](callback-decorator.md).
  
- on the line with marker `!(2)` we initialize `Global` object. 
`Global` can be briefly described as the main object for all daffi operations. This is entrypoint where you can specify what to initialize in this process (`Controller`,  `Node` or both), what kind of connection you want to have (Unix socket or TCP) and so on. 
  To know more about `Global` follow [this](global-object.md) link.
- on the line with marker `!(3)` we wait forever.

<hr>

##### consumer.py
```python
# consumer.py
import logging
from daffi import Global, FG

logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':

    g = Global(host="localhost", port=8888) # (1)!

    result = g.call.sum_of_two_numbers(5, 15) & FG # (2)!
    print(f"Result = {result}")

    g.stop() # (3)!
```
(This script is complete, it should run "as is")

- on the line with marker `!(1)` we initialize `Global` object to connect controller that was initialized
in `publisher.py` script. More details about nodes and controllers [here](node-and-controller.md)
- on the line with marker `!(2)` we call `sum_of_two_numbers` remote callback (remote call to `publisher.py`) with arguments `5` and `15`.
Then we use `FG` execution modifier that means we want to wait for result. More details about 
  available execution modifiers [here](execution-modifiers.md)
- after printing result, on the line with marker `!(3)` we stop `Global` object.

<hr>
To check how it works start `publisher.py` and `consumer.py` in two separate terminals
```bash
python3 publisher.py
python3 consumer.py
```

!!! note
    You can modify `publisher.py` and `consumer.py` scripts to try UNIX socket istead of connection via host/port.
    For this just delete host and port arguments from `Global` object initialization in both scripts.
    
    When host and port are not provided UNIX socket is default connection.


<hr>
Another important topic to cover is [fetcher](code-reference/fetcher.md) decorator.

You have already noticed that daffi makes remote calls through the `g` object using syntax:
```python
g.call.<remote callback name>(*args, **kwargs) & <execution modifier>
```

where `<remote callback name>` is the name of function registered in different process, `*args` and `**kwargs` is any arguments you want to pass to this function
and `<execution modifier>` is specific class that describes how to execute remote (single call, stream, broadcast, scheduled trigger etc) and how to wait for result (foregraund, background etc)

2 problems here:

- If your application is large, then you need to have the `g` object available in all places where remote callbacks are called.
- This syntax is not IDE friendly. IDE can't autocomplete or suggest you available callbacks on remote processes.

This is where `fetcher` comes in handy.

This decorator turns a local function into a pointer to a remote function by its name.

Let's change the `consumer.py` file so that it calls `sum_of_two_numbers` function not through the `g` object but through the `fetcher`

##### consumer.py
```python
# consumer.py
import logging
from daffi import Global, FG, fetcher, __signature_unknown__

logging.basicConfig(level=logging.INFO)


@fetcher # (1)!
def sum_of_two_numbers(a: int, b: int) -> int:
    __signature_unknown__(a, b)


if __name__ == '__main__':

    g = Global(host="localhost", port=8888)

    result = sum_of_two_numbers(5, 15) & FG # (2)!
    print(f"Result = {result}")

    g.stop() # (3)!
```
(This script is complete, it should run "as is")

- on the line with marker `!(1)` we register `fetcher` as pointer to remote `sum_of_two_numbers` callback.
<br>Decorated function name and signature must be the same as remote callback has. In function body we are using `__signature_unknown__` mock initialization.
Feel free to use `pass` statement instead of `__signature_unknown__`. Function's body doesn't make any sense
because after decorating it with `fetcher` it cannot be used for a local call.
<br>Although `__signature_unknown__` is more domain specific expression that emphasize purpose of this function. 
In addition, `__signature_unknown__` can take arbitrary positional and keyword arguments, so that all declared arguments will be used, which favorably affects the display in IDE.
- on the line with marker `!(2)` we call `sum_of_two_numbers` as if it were a local function with only one difference that after the call, the desired [execution modifier](execution-modifiers.md) must be specified.

<hr>
So far so good! Now you can check how it works by executing  `publisher.py` and `consumer.py` in two separate terminals
```bash
python3 publisher.py
python3 consumer.py
```
<hr>

At this moment we considered only `consumer` to `publisher` communication. In other words only `consumer` triggered remote callbacks. 

Lets modify `publisher.py` and `consumer.py` so that each of these processes registers a callback and triggers a callback from a neighboring process.

For example lets keep `sum_of_two_numbers` callback in `publisher.py` but create new function `consumer_time` in `consumer.py`.
 `consumer_time` should just return current UTC timestamp of consumer process.
 
 
##### publisher.py

```python
# publisher.py
import asyncio
import logging
from daffi import Global, callback, fetcher, __signature_unknown__, FG

logging.basicConfig(level=logging.INFO)


@callback  # (1)!
def sum_of_two_numbers(a: int, b: int) -> int:
    return a + b


@fetcher
async def consumer_time() -> float:
    __signature_unknown__()


async def runner(): # (2)!
    for _ in range(10):
        current_consumer_time = consumer_time() & FG
        print(f"Current consumer time: {current_consumer_time}")

        await asyncio.sleep(3)


if __name__ == '__main__':
    g = Global(host="localhost", port=8888, init_controller=True, process_name="publisher")  # (3)!

    g.wait_process("consumer") # (4)!

    asyncio.run(runner()) # (5)!

    g.stop()
```
(This script is complete, it should run "as is")


- (1)! Here we registered `sum_of_two_numbers` function as remote callback. Syntax is the same as in previous examples. 
- (2)! `runner` is async function that will be running in asyncio event loop. This function triggers `consumer_time` remote callback that is registered in `consumer.py` process. Execution happen to be 10 times in the cycle.
- (3)! Here we initialize `Global` object. In additional to provided host and port we also assign specific process name. This way we can wait process each other by name.
- (4)! Here we wait for `consumer` process to be started. 
- (5)! Start main `runner` function here


##### consumer.py

```python
# consumer.py
import asyncio
import logging
from datetime import datetime
from daffi import Global, FG, fetcher, __signature_unknown__, callback

logging.basicConfig(level=logging.INFO)


@callback # (1)!
async def consumer_time() -> float:
    return datetime.utcnow().timestamp()


@fetcher  # (2)!
async def sum_of_two_numbers(a: int, b: int) -> int:
    __signature_unknown__(a, b)


async def runner():  # (3)!
    for _ in range(10):
        result = sum_of_two_numbers(5, 15) & FG
        print(f"Result = {result}")

        await asyncio.sleep(3)


if __name__ == "__main__":

    g = Global(host="localhost", port=8888, process_name="consumer")  # (4)!

    g.wait_process("publisher")  # (5)!

    asyncio.run(runner())  # (6)!

    g.stop()
```
(This script is complete, it should run "as is")


- (1)! Here we registered `consumer_time` function as remote callback. 
- (2)! Here we created fetcher for `sum_of_two_numbers` function that is registered as callback in `publisher.py` process. 
- (3)! `runner` is async function that will be running in asyncio event loop. This function triggers `sum_of_two_numbers` remote callback that is registered in `publisher.py` process. Execution happen to be 10 times in the cycle.
- (4)! Here we initialize `Global` object. In additional to provided host and port we also assign specific process name. This way we can wait process each other by name.
- (5)! Here we wait for `publisher` process to be started. 
- (6)! Start main `runner` function here

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
from datetime import datetime
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


@callback_and_fetcher  # (1)!
async def get_process_name() -> str:
    return PROCESS_NAME


async def runner():  # (2)!
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

- (1)! We registered `get_process_name` function as `fetcher` and `callback`. It means this function is visible to other processes
 by name due to it is `callback` but if we trigger it locally it works as `fetcher` (trigger remote callback with name `get_process_name` on remote)
- (2)! We declared runner function which triggers `get_process_name` 10 times in cycle.


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


!!! note
    All examples above represents the simplest way processes (nodes) to communicate each other.
    Daffi controller can be registered as stand alone process as well. More details you can read in [node and controller](node-and-controller.md) section and [global object](global-object.md) section.
    

<hr>
You can check another examples at: [https://github.com/600apples/dafi/tree/main/examples](https://github.com/600apples/dafi/tree/main/examples)

