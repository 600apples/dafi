Execution modifiers are a set of classes and methods that indicate how the remote request should be executed
and how process executor expects to wait for computed result.

For instance lets consider following example:
```python
from daffi import Global, FG, NO_RETURN, BG

g = Global()

args = (1, 2, 3)
kwargs = dict(foo='bar')

result = g.call.my_remote_callback(*args, **kwargs) & FG
```

`& FG` in this example means process will wait until result is returned.

We can use another modifier:
```python
...
future = g.call.my_remote_callback(*args, **kwargs) & BG
...
# Do smth locally
result = future.get()
```
`& BG` means wait result 'in background'. This modifier returns instance of `AsyncResult`. You can take result whenever you want without blocking main process execution. 

Or we can say remote callback do not return result if we don't interested in it:
```python
...
g.call.my_remote_callback(*args, **kwargs) & NO_RETURN
...
# Do smth locally
...
```

#### list of available execution modifiers:

| Modifier class             | Description                                                           | Optional arguments  |
|------------------|-----------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `FG` | Stands for `foreground`. It means current process execution should be blocked until result of execution is returned.<br/>Another syntax:<br/> `g.call.<callback_name>(*args, **kwargs).fg(timeout=15)` | - `timeout`: The time to wait result. If exeeded `TimeoutError` will be thrown |
| `BG` | Stands for `background`. This modifier returns `AsyncResult` instance instead of result. Fits for long running tasks where caller execution cannot be blocked for a long time. <br/>Another syntax:<br/> `g.call.<callback_name>(*args, **kwargs).bg(timeout=15, eta=3)` | - `timeout`:  The time to wait result. If exeeded `TimeoutError` will be thrown <br/> - `eta`: The time to sleep in the background before sending execution request | 
| `NO_RETURN` | Use it if you don't need result of remote execution. <br/>Another syntax:<br/> `g.call.<callback_name>(*args, **kwargs).no_return(eta=3)` | - `eta`: The time to sleep in the background before sending execution request | `eta`: The time to sleep in the background before sending execution request |
| `PERIOD` | Use for scheduling reccuring tasks or tasks which should be executed several times. <br/>Another syntax:<br/> `g.call.<callback_name>(*args, **kwargs).period(at_time=[datetime.utcnow().timestamp() + 3, datetime.utcnow().timestamp() + 10])` | - `at_time`: One timestamp or list of timestamps. Timestamps should be according to utc time and it should be timestamp in the future. This argument forces remote callback to be triggered one or more times when when timestamp == datetime.utcnow().timestamp<br/> - `period`: Duration in seconds to trigger remote callback on regular bases. <br/> One can provide either `at_time` argument or `period` argument in one request. Not both! | 
| `BROADCAST` | Trigger all available callbacks on nodes by name. If `return_result` argument is set to True then aggregated result will be returned as dictionary where keys are node names and values are computed results. <br/>Another syntax:<br/> `g.call.<callback_name>(*args, **kwargs).broadcast(timeout=15, eta=3, return_result=True)` | - `timeout`:  The time to wait result. If exeeded `TimeoutError` will be thrown <br/> - `eta`: The time to sleep in the background before sending execution request <br/> `return_result`: If provided aggregated result from all nodes where callback exist will be returned. | 
| `STREAM` | Start streaming to one or more remote callbacks.  <br/>Another syntax:<br/> `g.call.<callback_name>(< Iterable >).stream()` | | 
