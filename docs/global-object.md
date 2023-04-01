[Global](code-reference/global.md) is the main initialization daffi entrypoint.


#### Global object initialization

You can initialize [Global](code-reference/global.md) object using the following syntax:
```python
from daffi import Global
g = Global(process_name='my awersome process', init_controller=True, host='localhost', port=8888)
```
 
where:
 
The `process_name` argument serves as an optional identifier for the process, 
allowing other processes to identify the initialized node by its given process_name.

If the process_name argument is not provided, a randomly generated name will be used. 
However, assigning meaningful names to nodes can be beneficial in certain scenarios. 
For example, one process may need to wait for another process by its specified name.

```python
g.wait_process('node name here')
```

`init_controller=True` Means we want to start `Controller` in this process.


`init_node` argument is True
by default so if you want to start only controller in particular process you should be explicit:


```python
g = Global(process_name=process_name, init_controller=True, init_node=False, host='localhost', port=8888)
```

`host` and `port` arguments give `Controller` and `Node` information how to connect and communicate.


`host` and `port` arguments are also optional.
For instance you can specify only `host`. In this case `Controller`/`Node` or both will be connected to random port

You can also skip these two arguments:
```python
g = Global(process_name=process_name, init_controller=True)
```
In this case `Controller`/`Node` will be connected using UNIX socket. By default UNIX socket is created within path
```bash
< temp directory >/daffi/.sock
```
Where `< temp directory >` is temporary directory of machine where 
`Controller`/`Node` is running. For instance it is going to be `/tmp/daffi/.sock` on Ubuntu.

You can also provide your own directory for UNIX socket:

```python
g = Global(process_name=process_name, init_controller=True, unix_sock_path="/foo/bar/biz")
```

#### Execution workflow

After initialization [Global](code-reference/global.md) object starts `Controller`/`Node` or both in separate thread.

You can join this thread to main process using `.join` of `.join_async` methods of Global


```python
g = Global(process_name=process_name, init_controller=True, init_node=False, host='localhost', port=8888)
g.join()

#--- or `join_async` if your application is asynchronous
await g.join_async() 
```

`Controller`/`Node` or both can be terminated by calling the `stop` method. 
This method is particularly useful for short-lived jobs, 
such as starting a Daffi process, triggering a few callbacks on other nodes, and then terminating the process.

 
```python
g = Global(process_name=process_name, init_controller=True, init_node=False, host='localhost', port=8888)
# .....
# Execute remote callbacks ...
g.stop() 
```

For this reason [Global](code-reference/global.md) can be used as context manager. `stop` method is executed explicitly
on exit from context manager scope.


```python
with Global(process_name=process_name, init_controller=True, init_node=False, host='localhost', port=8888) as g:
    # .....
    # Execute remote callbacks ...
```

!!! warning
    Dont use `.stop` method to stop daffi components in long living applications eg web servers, background workers etc.
    start/stop daffi components requires some initialization time and resources.

#### waiting for nodes or methods to be available

Sometimes nodes start at different times and because of this, some remote callbacks may not be available immediately.

[Global](code-reference/global.md) has several methods to control waiting for callbacks availability.

The 2 examples below illustrate waiting for a remote process to be available:

```python
g.wait_process('name of remote node')
```
or

```python
await g.wait_process_async('name of remote node')
```


[Global](code-reference/global.md) can also wait a specific callback to be available by its name:


```python
g.wait_function('name of remote callback')
```
or

```python
await g.wait_function_async('name of remote callback')
```

Waiting by callback name criteria can be useful when many nodes contain a callback with the same name and we need to wait for the presence of one of them


#### Transfer and execute function on remote Node

This option is suitable when you want to create remote callback dynamically and execute them on remote nodes.

Example:

process 1
```python
import logging
from daffi import Global

logging.basicConfig(level=logging.INFO)


def main():
    Global(process_name="other_process", init_controller=True, host="localhost", port=8888).join()


if __name__ == "__main__":
    main()
```
(This script is complete, it should run "as is")


process 2
```python
import logging
from daffi import Global

logging.basicConfig(level=logging.INFO)


async def func_to_transfer():
    """Return pid id of remote process"""
    import os

    return os.getpid()


def main():

    with Global(host="localhost", port=8888) as g:
        
        remote_pid = g.transfer_and_call(remote_process="other_process", func=func_to_transfer)
        print(f"Remote process pid: {remote_pid}")


if __name__ == "__main__":
    main()
```
(This script is complete, it should run "as is")

On example above we transfer function `func_to_transfer` from process2 to process1 and execute it.
The result of function execution will be returned to process2. 

!!! warning
    You should make sure all imports that are using in function body are available on remote process.
    Otherwise you should consider import modules in function body (like `import os` in example above).


#### Working with scheduled tasks

[Global](code-reference/global.md) has methods `get_scheduled_tasks` and `cancel_scheduled_task_by_uuid` 
to works with scheduled tasks (see [scheduling tasks](usage/scheduling-tasks.md)) on remote process.

- `get_scheduled_tasks` get all scheduled tasks on remote process by process name.The method returns all task UUIDs that are currently scheduled on the remote process.

- `cancel_scheduled_task_by_uuid` cancel one scheduled task on remote process by its UUID. All UUIDs can be obtained using `get_scheduled_tasks` method.
