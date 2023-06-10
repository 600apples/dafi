Using a special class modifier called `PERIOD`, 
it is possible to initiate recurring callback executions or schedule a group of delayed callback executions simultaneously.

`PERIOD` take 2 optional arguments `at_time` and `interval` but only one of them is allowed to be provided.


#### Example

=== "class based approach"

    `scheduler.py` content:
    ```python
    import time
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class Scheduler(Callback):
        auto_init = True
    
        def long_running_task1(self):
            print("Start long running task 1")
            for i in range(1, 3):
                print(f"Processing item {i}...")
                time.sleep(1)
            print("Task 1 complete.")
    
        def long_running_task2(self):
            print("Start long running task 2")
            for i in range(100, 103):
                print(f"Processing item {i}...")
                time.sleep(1)
            print("Task 2 complete.")
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `scheduler_client.py` content:
    ```python
    import time
    import logging
    from datetime import datetime
    from daffi import Global, PERIOD
    from daffi.registry import Fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    class SchedulerClient(Fetcher):
        # The default behavior of this fetcher is to execute each of its methods every 5 seconds.
        exec_modifier = PERIOD(interval="5s")
    
        def long_running_task1(self):
            pass
    
        def long_running_task2(self):
            pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
        scheduler_client = SchedulerClient()
    
        task_ident = scheduler_client.long_running_task1()
        time.sleep(30)
    
        # Stop the current recurring task.
        task_ident.cancel()
    
        now = datetime.utcnow().timestamp()
        at_time = [now + 10, now + 30]
    
        # Execute the `long_running_task2` function twice on the remote.
        # The first execution will occur 10 seconds from now, and the second will occur 30 seconds from now.
        task_ident = scheduler_client.long_running_task2.call(exec_modifier=PERIOD(at_time=at_time))
    
        g.stop()
    ```
    
    Execute in two separate terminals:
    ```bash
    python3 scheduler.py
    python3 scheduler_client.py
    ```

=== "decorator based approach"

    `scheduler.py` content:
    ```python
    import time
    import logging
    from daffi import Global
    from daffi.decorators import callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    
    @callback
    def long_running_task1():
        print("Start long running task 1")
        for i in range(1, 3):
            print(f"Processing item {i}...")
            time.sleep(1)
        print("Task 1 complete.")
    
    
    @callback
    def long_running_task2():
        print("Start long running task 2")
        for i in range(100, 103):
            print(f"Processing item {i}...")
            time.sleep(1)
        print("Task 2 complete.")
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888).join()
    ```
    
    `scheduler_service.py` content:
    ```python
    import time
    import logging
    from datetime import datetime
    from daffi import Global, PERIOD
    from daffi.decorators import fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    @fetcher(exec_modifier=PERIOD(interval="5s"))
    def long_running_task1():
        """Execute this method every 5 second on remote"""
        pass
    
    
    @fetcher
    def long_running_task2():
        """The default execution modifier is set here, but it can be changed at runtime."""
        pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888)
    
        task_ident = long_running_task1()
        time.sleep(30)
    
        # Stop the current recurring task.
        task_ident.cancel()
    
        now = datetime.utcnow().timestamp()
        at_time = [now + 10, now + 30]
    
        # Execute the `long_running_task2` function twice on the remote.
        # The first execution will occur 10 seconds from now, and the second will occur 30 seconds from now.
        task_ident = long_running_task2.call(exec_modifier=PERIOD(at_time=at_time))
    
        g.stop()
    ```

    Execute in two separate terminals:
    ```bash
    python3 scheduler.py
    python3 scheduler_client.py
    ```

#### `at_time` argument

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

#### `interval` argument

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
