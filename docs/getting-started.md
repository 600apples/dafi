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
Describing  `callback` in other words, it makes the function `sum_of_two_numbers` visible to all other processes where daffy is running.
More details about using `callback` decorator you can find [here](callback-decorator.md).
  
- on the line with marker `!(2)` we initialize `Global` object. 
`Global` can be briefly described as the main object for all daffi operations. This is entrypoint wher you can specify what to initialize in this process (`Controller`,  `Node` or both), what kind of connection you want to have (Unix socket or TCP) and so on. 
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

You can check another examples at: [https://github.com/600apples/dafi/tree/main/examples](https://github.com/600apples/dafi/tree/main/examples)

