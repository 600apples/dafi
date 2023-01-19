It is possible to start reccuring callback execution or set bunch of delayed callback executions at once.

For this reason special class modifier `PERIOD` is used.

`PERIOD` take 2 optional arguments `at_time` and `interval` but only one of them is allowed to be provided.

#### at_time

- `at_time` - take one timestamp or list of timestamps. They all should be timestamps related to UTC time (and timestamps in the future).

For instance lets consider you want to execute remote callback `my_callback` 3 times, After 2 seconds after 10 seconds and after a minute:

```python
from datetime import datetime
from daffi import Global, PERIOD

g = Global(host="localhost", port=8888)

# Wait my_callback function to be available on remote
g.wait_function("some_func")

now = datetime.utcnow().timestamp()

# 2 sec, 10 sec and 60 sec later
at_time = [now + 2, now + 10, now + 60]

task = g.call.some_func() & PERIOD(at_time=at_time)
```

Execution with `PERIOD` modifier returns `ScheduledTask` instance as result of execution.

You can cancel all tasks triggered from this execution:
```python
...
at_time = [now + 2, now + 10, now + 60]
task = g.call.some_func() & PERIOD(at_time=at_time)

... # do smth
time.sleep(3)
task.cancel()
```

As we slept 3 seconds before canceling only two executions that remains in `at_time` bunch will be canceled as first of them was already triggered earlier.

#### interval

Using `interval` has the same execution signature and also returns instance of `ScheduledTask` as result so you can cancel reccuring task any time you want

```python
import time
from datetime import datetime
from daffi import Global, PERIOD

g = Global(host="localhost", port=8888)

# Wait my_callback function to be available on remote
g.wait_function("some_func")

now = datetime.utcnow().timestamp()

# Execute remote callback `some_func` each 5 seconds
task = g.call.some_func() & PERIOD(interval=5)

time.sleep(60)
task.cancel()
```

On example above we started recurring execution each 5 second and canceled it after sleep

!!! note
    interval also takes special string formatted expressions as values
    for instance next statement is also valid:
    
    ```python
    task = g.call.some_func() & PERIOD(interval="5s")
    ```
    
    Another examples:
    
    '1m24s'       ==  84 seconds
    
    '1.2 minutes' == 72 seconds
