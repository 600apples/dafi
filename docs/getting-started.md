Let's create simple application.

This application will consist of two python files `publisher.py` and `consumer.py`.

- `publisher.py` will register a simple function `sum_of_two_numbers` as a remote callback. This function should take 2 numbers and return their sum.
- `consumer.py` will call the function `sum_of_two_numbers` remotely passing it 2 numbers as arguments,  wait for the result and print it.


```python
# publisher.py
from daffi import Global, callback


@callback # (1)!
def sum_of_two_numbers(a: int, b: int) -> int:
    return a + b


if __name__ == '__main__':
    
    g = Global(host="localhost", port=8888, init_controller=True) # (2)!
    g.join() # (3)!
```
(This script is complete, it should run "as is")

- on the line with marker `!(1)` we use `callback` decorator which registers the function as a remote callback.
More details about using `callback` decorator you can find [here](callback-decorator.md).
  
- on the line with marker `!(2)` we initialize `Global` object. 
`Global` can be briefly described as the main object for all daffi operations. This is entrypoint wher you can specify what to initialize in this process (`Controller`,  `Node` or both), what kind of connection you want to have (Unix socket or TCP) and so on. 
  To know more about `Global` follow [this](global-object.md) link.
- on the line with marker `!(3)` we wait forever.

<hr>
```python
# consumer.py
from daffi import Global, FG


if __name__ == '__main__':

    g = Global(host="localhost", port=8888) # (1)!

    result = g.call.sum_of_two_numbers(5, 15) & FG # (2)!
    print(f"Result = {result}")

    g.stop() # (3)!
```
(This script is complete, it should run "as is")

- on the line with marker `!(1)` we initialize `Global` object to connect controller that was initialized
in `publisher.py` script. More details about nodes and controllers [here](node-and-controller.md)
- on the line with marker `!(2)` we call `sum_of_two_numbers` remote callback with arguments `5` and `15`.
Then we use special `FG` execution modifier that means we want to wait for result. More details about 
  available execution modifiers [here](execution-modifiers.md)
- after printing result on the line with marker `!(3)` we stop `Global` object.

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


You can check another examples at: [https://github.com/600apples/dafi/tree/main/examples](https://github.com/600apples/dafi/tree/main/examples)

