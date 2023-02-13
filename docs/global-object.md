[Global](code-reference/global.md) is the main initialization daffi entrypoint.


#### Global initialization

You can initialize [Global](code-reference/global.md) object using the following syntax:
```python
from daffi import Global
g = Global(process_name='my awersome process', init_controller=True, host='localhost', port=8888)
```
 
where:
 
`process_name` is optional `Node` identificator. Other nodes see initialized node by its `process_name`
 

If `process_name` argument is omitted then randomly generated name will be used.
But in some cases it is helpful to give nodes meaningful names. 
For example one process can wait another process by its name:

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

#### Execucution workflow

After initialization [Global](code-reference/global.md) object starts `Controller`/`Node` or both in separate thread.

You can join this thread to main process using `.join` of `.join_async` methods of Global


```python
g = Global(process_name=process_name, init_controller=True, init_node=False, host='localhost', port=8888)
g.join()

#--- or `join_async` if your application is asynchronous
await g.join_async() 
```

`Controller`/`Node` or both can be stopped using `.stop` method of Global. This method is suitable for short living jobs
 for example when you want to start daffi process, trigger couple of callbacks on other nodes and terminate process.

 
```python
g = Global(process_name=process_name, init_controller=True, init_node=False, host='localhost', port=8888)
# .....
# Execute remote callbacks ...
await g.stop() 
```

For this reason [Global](code-reference/global.md) can be used as context manager. `.stop` method is executed explicitly
on exit from context manager scope.


```python
with Global(process_name=process_name, init_controller=True, init_node=False, host='localhost', port=8888) as g:
    # .....
    # Execute remote callbacks ...
```

!!! warning
    Dont use `.stop` method to stop daffi components in long living applications eg web servers, background workers etc.
    start/stop daffi components requires some initialization time and resources so it is better to initialize Global once
    and join it

#### waiting for nodes or methods to be available

Sometimes nodes start at different times and because of this, some remote callbacks may not be available immediately.

[Global](code-reference/global.md) has several methods to control waiting for callbacks availability.

The 2 examples below illustrate waiting for a remote node to be available:

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
to works with scheduled tasks on remote Node. To know more about scheduled tasks go [here](scheduling-tasks.md) 

- `get_scheduled_tasks` get all scheduled tasks on remote Node by Node name. This method returns all task uuids that are currently scheduled on remote Node.
- `cancel_scheduled_task_by_uuid` cancel one scheduled task on remote Node by its uuid. All uuids can be obtained using `get_scheduled_tasks` method.


#### remote callbacks execution with g object

!!! warning
    This syntax is appropriate for tiny microservices only. In general you should consider using 
    [fetcher](./fetcher-decorator.md) decorator to trigger remote callback as it is more self explanatory syntax option.

once you have the `g` object initialized you can call the remote callback. The general syntax is:

```python
...
g.call.< remote callback name >(*args, **kwargs) & < execution modifier >
```

- `remote callback name` is the name of function registered as remote callback on different `Node`
- `execution modifier` is modifier class name that determines how to execute remote callback and what to do with result. More details about execution modifiers [here](execution-modifiers.md)
