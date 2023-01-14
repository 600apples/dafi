Lets create very simple apps based on classic one-to-one communication
where the `Controller` is running together with one of the nodes:

![image.png](images/one-to-one.png)
`Process 1` can trigger remote callbacks `func1` and `func2` from `Process2`. Communication between `Node` runinning in `Process1` and `Node` running in `Process2` is happen to be through `Controller` running in `Process1` 


![one-to-one-reverse](images/one-to-one-reverse.png) 
<br /><br /><br /><br /><br /><br />
---
But if `func3` is registered as callback in `Process1` and `Process1` has `Node` running  this also means that `Process2` can call this callback at the same time.

<br /><br /><br /><br /><br /><br /><br /><br /><br />

---

Let's see the full example first, and we'll break it down piece by piece
<br /><br />

Process1:
```python
import time
import threading
from daffi import Global, callback, FG, NO_RETURN


@callback
def func3(a: int, b: int) -> int:
    """Add 2 numbers and return sum"""
    # Simulate long running job
    time.sleep(3)
    return a + b


def remote_process_executor(g: Global):
    """Execute remote callbacks"""

    for _ in range(10):
        delta = g.call.func1(10, 3) & FG

        print(f'Calculated delta = {delta}')

        g.call.func2() & NO_RETURN

        time.sleep(5)


def main():
    """Main entrypoint"""
    process_name = 'proc1'

    g = Global(process_name=process_name, init_controller=True, host='localhost', port=8888)

    g.wait_process('proc2')

    re = threading.Thread(target=remote_process_executor, args=(g,))
    re.start()
    re.join()

    g.stop()


if __name__ == '__main__':
   main()
```
(This script is complete, it should run "as is")



Process2:
```python
"""Lets make process 2 asynchronous! Daffi works great with any type of applications"""
import asyncio
from daffi import Global, callback, BG


@callback
async def func1(a: int, b: int) -> int:
    """Subtracts 2 numbers and return delta"""
    return a - b


@callback
def func2() -> None:
    """Just print text to console"""
    print('Func2 has been triggered!')



async def remote_process_executor(g: Global):
    """Execute remote callbacks"""
    for _ in range(5):
        future = g.call.func3(4, 6) & BG

        # Do smth while result is processing
        await asyncio.sleep(2)

        # Get result
        result = await future.get_async()
        print(f"Calculated sum is {result!r}")


async def main():
    """Main entrypoint"""

    process_name = 'proc2'

    g = Global(process_name=process_name, host='localhost', port=8888)
    asyncio.create_task(remote_process_executor(g))

    # Wait forever
    await g.join_async()


if __name__ == '__main__':
   asyncio.run(main())
```
(This script is complete, it should run "as is")


### Explanation

In `Process1` we initialize [Global](code-reference/global.md) object to use TCP connection and init controller in this process.
```python
g = Global(process_name=process_name, init_controller=True, host='localhost', port=8888)
```
Then we start `remote_process_executor` function in separate thread and wait this thread to be complete.
```python
re = threading.Thread(target=remote_process_executor, args=(g,))
re.start()
re.join()
```

`remote_process_executor` call `func1` and `fund2` callbacks of `Process2` 10 times. For one of them it uses execution modifier `FG` to take result
and for second use modifier `NO_RETURN` to skip result.
```python
for _ in range(10):
    delta = g.call.func1(10, 3) & FG

    print(f'Calculated delta = {delta}')

    g.call.func2() & NO_RETURN

    time.sleep(5)
```

And last step we stop [Global](code-reference/global.md) connection:
```python
g.stop()
```

`stop` method make sense only for short living applications like periodic jobs etc. 
For long running applications you can use `g.join()` or `g.join_async()` if you have async application.


In contrast to `Process1`, `Process2` script use asyncio. Daffi can work with async code as well.

[Global](code-reference/global.md) instance initialization looks like this:

```python
g = Global(process_name=process_name, host='localhost', port=8888)
```

If `init_controller=True` argument is skipped then only `Node` will be initialized in current process.
Also `Process2` sleeps forever on next line: 

```python
await g.join_async()
```

To know more about async daffi capabilities you should check this [section](async-apps.md)

