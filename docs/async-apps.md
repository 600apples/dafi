Most of daffi methods works fine with both sync and async applications but some of them blocks event loop. 

Blocking methods have async conterparts which should be used for asynchronous processes.

For example [Global](code-reference/global.md) object has method `join` which is blocking method and cannot be used for applications 
which leverage on event loop. 

But `Global` also has `join_async` method which fits async non blocking model.

Also there are two methods `get` and `get_async` to take result depending on application tipe.

```python
result = await future.get_async()
```

In synchronous applications there is method `get` does this job.

Another methods category is generic methods. For instance execution modifiers can be sync or async depends on situation:
```python
delta = g.call.func1(10, 3) & FG
```

For async applications it is also possible to use this statement as coroutine:
```python
delta = await g.call.func1(10, 3) & FG
```

But async modifiers option has certain limitation. For example following syntax is not valid:
```python
 fg = FG(timeout=10)
delta = await g.call.func1(10, 3) & fg
```

Only one-liner expression will be treated as coroutine:
```python
delta = await g.call.func1(10, 3) & FG(timeout=10)
```
