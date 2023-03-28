Execution modifiers are set of classes that specify the manner 
in which the remote request should be carried out, as well as how the process executor should anticipate the resulting computation.


#### list of available execution modifiers:

| Modifier class             | Description                                                           | Optional arguments  |
|------------------|-----------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `FG` | Stands for `foreground`. It means current process execution should be blocked until result of execution is returned | - `timeout`: The time to wait result. If exeeded `TimeoutError` will be thrown |
| `BG` | Stands for `background`. This modifier returns `AsyncResult` instance instead of result. Fits for long running tasks where caller execution cannot be blocked for a long time. | - `timeout`:  The time to wait result. If exeeded `TimeoutError` will be thrown <br/> - `eta`: The time to sleep in the background before sending execution request <br/> - `return_result`: boolean flag that indicates if we want to receive result of remote callback execution. | 
| `PERIOD` | Use for scheduling reccuring tasks or tasks which should be executed several times.  | - `at_time`: One timestamp or list of timestamps. Timestamps should be according to utc time and it should be timestamp in the future. This argument forces remote callback to be triggered one or more times when timestamp == datetime.utcnow().timestamp<br/> - `period`: Duration in seconds to trigger remote callback on regular bases. <br/> One can provide either `at_time` argument or `period` argument in one request. Not both! | 
| `BROADCAST` | Trigger all available callbacks on nodes by name. If `return_result` argument is set to True then aggregated result will be returned as dictionary where keys are node names and values are computed results. | - `timeout`:  The time to wait result. If exeeded `TimeoutError` will be thrown <br/> - `eta`: The time to sleep in the background before sending execution request <br/> `return_result`: If provided aggregated result from all nodes where callback exist will be returned. | 
