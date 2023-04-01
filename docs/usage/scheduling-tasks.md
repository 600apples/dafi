Using a special class modifier called `PERIOD`, 
you can initiate recurring callback executions or schedule a group of delayed callback executions simultaneously.

`PERIOD` take 2 optional arguments `at_time` and `interval` but only one of them is allowed to be provided.

#### at_time

You can provide either a single timestamp or a list of timestamps, all of which must be related to UTC time and in the future.
 
For example, if you want to execute the remote callback `some_func` three times - 2 seconds from now, 10 seconds from now, and 1 minute from now - you would specify these three timestamps:

```python
import logging
from datetime import datetime
from daffi import Global, PERIOD
from daffi.decorators import fetcher

logging.basicConfig(level=logging.INFO)


g = Global(host="localhost", port=8888)


@fetcher
def some_func():
    pass


# Wait my_callback function to be available on remote
g.wait_function("some_func")

now = datetime.utcnow().timestamp()

# 2 sec, 10 sec and 60 sec later
at_time = [now + 2, now + 10, now + 60]

task = some_func.call(exec_modifier=PERIOD(at_time=at_time))
```

Execution with `PERIOD` modifier returns `ScheduledTask` instance as result of execution.

You can cancel all tasks triggered from this execution:
```python
...
time.sleep(3)
task.cancel()
```

As we slept 3 seconds before canceling only two executions that remains in `at_time` bunch will be canceled as first of them was already triggered earlier.

#### interval

Using `interval` has the same execution signature and also returns instance of `ScheduledTask` as result so you can cancel reccuring task any time you want

`PERIOD` with `interval` argument has the same execution signature and also returns an instance of `ScheduledTask`, allowing you to cancel the recurring task at any time.


```python
import time
import logging
from datetime import datetime
from daffi import Global, PERIOD
from daffi.decorators import fetcher

logging.basicConfig(level=logging.INFO)


@fetcher(PERIOD(interval=5))
def some_func():
    pass


g = Global(host="localhost", port=8888)

# Wait my_callback function to be available on remote
g.wait_function("some_func")

now = datetime.utcnow().timestamp()

# Execute remote callback `some_func` each 5 seconds
task = some_func()
time.sleep(60)
task.cancel()
```

On example above we started recurring execution each 5 second and canceled it after sleep

!!! note
    interval also takes special string formatted expressions as values
    for instance next statement is also valid:
    
    PERIOD(interval="5s")

    Another examples:
    
    PERIOD(interval="1m24s")  # == 84 seconds
    
    PERIOD(interval="1.2 minutes")  # == 72 seconds
