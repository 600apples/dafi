fetcher decorator is decorator for create `pointers` to remote callbacks.

Lets say we declared callback with name `trigger_me` in process-1


process-1
```python
from daffi import callback


@callback
def trigger_me(a: int, b: int):
    return a + b
# ...
```

Then in process-2 we can create pointer to it using `fetcher` decorator


process-2
```python
from daffi import fetcher, FG, __body_unknown__

@fetcher(FG)
def trigger_me(a: int, b: int):
    __body_unknown__(a, b)
# ...
```

After [Global](./global-object.md) object initialization we can trigger this pointer like it is local function

```python
result = trigger_me(1, 2)
```

But instead of local execution remote callback `trigger_me` in `process-1` will be triggered and result will be delivered to `process-2`


[Execution modifier](./execution-modifiers.md) `FG` is optional argument for `fetcher`. If it is not specified in `fetcher` arguments it should be provided during 
fetcher execution


process-2
```python
from daffi import fetcher, FG, __body_unknown__

@fetcher
def trigger_me(a: int, b: int):
    __body_unknown__(a, b)
# ...

result = trigger_me(4, 5) & FG
```

By default function body for function, decorated with `fetcher` is not used. That is why we use `__body_unknown__` placeholder (which can be simply replaced with `pass` statement)

When function body is not used then all arguments provided during fetcher execution will be reflected to remote callback. It is important to keep callback/fetcher arguments consistency.

But one can specify `args_from_body` argument during fetcher initialization. In this case `fetcher`'s return statement will be used as arguments for callback

At this moment only tuple or single return value is valid syntax for fetcher with `args_from_body=True`.

process-2
```python
from daffi import fetcher, FG

@fetcher(FG, args_from_body=True)
def trigger_me(a: int, b: int):
    return a * 5, b * 5
# ...

result = trigger_me(4, 5)
```

!!! note
    In case remote callback expects only 1 argument you can return single value from [fetcher](code-reference/fetcher.md).
