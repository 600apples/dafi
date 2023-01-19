[Global](code-reference/global.md) is the main initialization entrypoint and remote callbacks executor at the same time.

#### Global initialization

You can initialize [Global](code-reference/global.md) object using the following syntax:
```python
from daffi import Global
g = Global(process_name='my awersome process', init_controller=True, host='localhost', port=8888)
```
 
where:
 
`process_name` is optional `Node` identificator. 
 

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

`host` and `port` arguments give `Controller` and `Node` information how to connect.


`host` and `port` arguments are also optional.
For instance you can specify only `host`. In this case `Controller`/`Node` or both will be connected to random port

You can also skip these two arguments:
```python
g = Global(process_name=process_name, init_controller=True)
```
In this case `Controller`/`Node` will be connected using UNIX socket. By default UNIX socket is created using path
```bash
< temp directory >/daffi/.sock
```
Where `< temp directory >` is temporary directory of machine where `Controller`/`Node` is running. For instance it is going to be `/tmp/daffi/.sock` on Ubuntu.

You can also provide your own directory for UNIX socket:

```python
g = Global(process_name=process_name, init_controller=True, unix_sock_path="/foo/bar/biz")
```

#### remote callbacks execution with g object

once you have the `g` object initialized you can call the remote callback. The general syntax is:


```python
...
g.call.< remote callback name >(*args, **kwargs) & < execution modifier >
```

- `remote callback name` is the name of function registered as remote callback on different `Node`
- `execution modifier` is modifier class name that determines how to execute remote callback and what to do with result. More details about execution modifiers [here](execution-modifiers.md)

#### waiting for nodes or methods to be available

Sometimes nodes start at different times and because of this, some remote callbacks may not be available immediately.

[Global](code-reference/global.md) has several methods to control waiting for callbacks availability:


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
