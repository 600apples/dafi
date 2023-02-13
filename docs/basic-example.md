To give you a flavour of how it works let's create simple application.

This application will consist of two python files `publisher.py` and `consumer.py`.

- `publisher.py` will register a simple function `sum_of_two_numbers` as a remote callback. This function should take 2 numbers and return their sum.
- `consumer.py` will call the function `sum_of_two_numbers` remotely passing it 2 numbers as arguments,  wait for the result and print it.

##### publisher.py

In `publisher.py` we need to create function `sum_of_two_numbers` and make it visible for `consumer.py`. 
For this reason [callback](code-reference/callback.md) decorator is used.

```python
import logging
from daffi import Global, callback

logging.basicConfig(level=logging.INFO)


@callback # (1)
def sum_of_two_numbers(a: int, b: int) -> int:
    return a + b


if __name__ == '__main__':
    
    g = Global(host="localhost", port=8888, init_controller=True) # (2)
    g.join() # (3)
```
(This script is complete, it should run "as is")

- `(1)` we use `callback` decorator which registers the function as a remote callback.
It makes `sum_of_two_numbers` visible to all other processes where daffi is running.
More details about using `callback` decorator you can find [here](callback-decorator.md).
  
- `(2)` we initialize `Global` object. 
  To know more about `Global` follow [this](global-object.md) link. It is worth to mention that we use `init_controller=True` here. Among several daffi processes we need to have one 
  where `Controller` is initialized. `Controller` in daffi teminology is server. `Controller` can be stand-alone process or be combined with `Node` (as in this example). More details about nodes and controllers [here](node-and-controller.md)
- `(3)` we join [Global](code-reference/global.md) instance to main thread. By default [Global](code-reference/global.md) runs daffi components in separate thread.

<hr>

##### consumer.py
In `consumer.py` we need to describe how to execute `sum_of_two_numbers` registered in `publisher.py`. For this reason [fetcher](code-reference/fetcher.md) is used.
<br/>You can initialize and execute [fetcher](code-reference/fetcher.md) in different ways. Les't go trough some of them:

```python
import logging
from daffi import Global, FG, fetcher, __body_unknown__

logging.basicConfig(level=logging.INFO)


@fetcher(exec_modifier=FG) # (1)
def sum_of_two_numbers(a: int, b: int) -> int:
    __body_unknown__(a, b)


if __name__ == '__main__':

    g = Global(host="localhost", port=8888) # (2)

    result = sum_of_two_numbers(5, 15) # (3)
    print(f"Result = {result}")

    g.join()
```
(This script is complete, it should run "as is")

- `(1)` we register `fetcher` as pointer to remote `sum_of_two_numbers` callback.
<br/>Argument `exec_modifier=FG` means each time we trigger `fetcher` we want to wait for result. More details about available execution modifiers [here](execution-modifiers.md) 
<br>In this particular example decorated function name and signature must be the same as remote callback has.
<br/>In function body we are using `__body_unknown__` mock. [fetcher](code-reference/fetcher.md)
doesn't use function body by default. Only function name and function signature (arguments) are used to trigger remote.
Feel free to use `pass` statement instead of `__body_unknown__`.
- `(2)` we initialize  [Global](code-reference/global.md). Port and host must correspond to port and host of `Controller` that we initialized in `publisher.py`.
- `(3)` we trigger `sum_of_two_numbers`  [fetcher](code-reference/fetcher.md). Arguments passed to this fetcher will be transferred to `publisher.py` process
where [callback](code-reference/callback.md) `sum_of_two_numbers` will be triggered.

<hr>
To check how it works start `publisher.py` and `consumer.py` in two separate terminals
```bash
python3 publisher.py
python3 consumer.py
```

<hr>

There is also another argument `args_from_body` you can pass to [fetcher](code-reference/fetcher.md).
In this case result returned from [fetcher](code-reference/fetcher.md)'s body is using as arguments to pass to remote callback.

Let's modify `consumer.py`:

```python
import logging
from typing import Tuple
from daffi import Global, FG, fetcher

logging.basicConfig(level=logging.INFO)


@fetcher(exec_modifier=FG, args_from_body=True) # (1)
def sum_of_two_numbers(multiplier: int) -> Tuple[int, int]:
    arg1 = 5 * multiplier
    arg2 = 10 * multiplier
    return arg1, arg2


if __name__ == '__main__':

    g = Global(host="localhost", port=8888)

    result = sum_of_two_numbers(multiplier=2)
    print(f"Result = {result}")

    g.join()
```
(This script is complete, it should run "as is")


- `(1)` we register [fetcher](code-reference/fetcher.md) with `args_from_body=True` argument. 
<br/>In this example only decorated function name should be the same as name of remote callback.
If `args_from_body=True` argument is provided then we can use any arguments want.<br>
But return statement of decorated function must return tuple of 2 values `(a: int, b: int)` that corresponds to arguments specified in [callback](code-reference/callback.md)  `sum_of_two_numbers` signature.


!!! note
    In case remote callback expects only 1 argument you can return single value from [fetcher](code-reference/fetcher.md).
    

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

##### Another fetcher syntax examples

[fetcher](code-reference/fetcher.md) is quite flexible. 

You can specify `exec_modifier` argument in decorator or skip it completely.

In this case you should provide [execution modifier](execution-modifiers.md) during `fetcher` execution.

This way is appropriate when you need to trigger remote callback with different execution modifiers depends on situation.

```python
import logging
from daffi import Global, fetcher, __body_unknown__, FG, BROADCAST, STREAM

logging.basicConfig(level=logging.INFO)

stream_values = [("a", "b"), ("c", "d"), ("f", "g"), ("y", "z")]


@fetcher
def my_awersome_func(a: str, b: str) -> str:
    __body_unknown__(a, b)
    

if __name__ == '__main__':

    g = Global(host="localhost", port=8888)
    
    # Trigger one `my_awersome_func` callback on remote and wait for result
    result = my_awersome_func(a="foo", b="bar") & FG
    print(f"Result = {result}")

    # Trigger all `my_awersome_func` callbacks registered on remote nodes (broadcast)
    my_awersome_func(a="foo", b="bar") & BROADCAST

    # Stream to all `my_awersome_func` callbacks registered on remote nodes
    my_awersome_func(stream_values) & STREAM
    g.join()
```

!!! note
    [execution modifiers](execution-modifiers.md) take various arguments and can be passed to fetcher as class or class instance.
    For example `BG` execution modifier has 2 arguments `timeout` and `eta`. `timeout` is used when you want to limit execution time on remote. If this value exceeded then `TimeoutError` will be thrown. `eta` describes delay in seconds before remote callback execution.
    <br>So you can consider two options:
    ```python
    from daffi import Global, fetcher, BG
    
    @fetcher(BG)
    def my_awersome_func(a: str, b: str) -> str:
        __body_unknown__(a, b)  
    ...
    # BG execution modifier returns instance of `AsyncResult`. To obtain result we need to use .get() method of `AsyncResult`
    future = my_awersome_func(1, 2)
    result = future.get()
    ```
    (fetcher without timeout and without eta.)
    
    ```python
    from daffi import Global, fetcher, BG
    
    fetcher(BG(timeout=10, eta=2))
    def my_awersome_func(a: str, b: str) -> str:
        __body_unknown__(a, b)  
    ...
    # BG execution modifier returns instance of `AsyncResult`. To obtain result we need to use .get() method of `AsyncResult`
    future = my_awersome_func(1, 2)
    result = future.get()
    
    ```
    (fetcher waits for result no more then 10 seconds and trigger callback execution after 2 second of delay(eta))
        
  
<hr>

You can also trigger execute remote callback through [Global](global-object.md) object without [fetcher](code-reference/fetcher.md) registration.
General syntax can be described as the following:

```python
g.call.<remote callback name>(*args, **kwargs) & <execution modifier>
```

where `<remote callback name>` is the name of function registered in different process, `*args` and `**kwargs` is any arguments you want to pass to this function
and `<execution modifier>` is specific class that describes how to execute remote (single call, stream, broadcast, scheduled trigger etc) and how to wait for result (foregraund, background etc)

Example:

```python
import logging
from daffi import Global, FG

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':

    g = Global(host="localhost", port=8888)

    result = g.call.my_awersome_func(a="foo", b="bar") & FG
    print(f"Result = {result}")

    g.join()
```

This syntax has 2 problems:

- If your application is large, then you need to have the `g` object available in all places where remote callbacks are called.
- This syntax is not IDE friendly. IDE can't autocomplete or suggest you available callbacks on remote processes.

But it still can be considered for tiny microservices architecture.

<hr>
