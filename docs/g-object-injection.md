One can create and register callback that takes special argument `g`


```python
from daffi import callback, Global

@callback
def my_callback(g: Global, **kwargs):
    ...
```

!!! warning
    `g` is reserved argument name and cannot be used to pass arbitrary data
    


In fact `g` argument works like [fixture](https://docs.pytest.org/en/6.2.x/fixture.html) in pytest. 
Once `g` is specified as argument on remote callback it will be automatically populated with [Global](code-reference/global.md) instance on remote process.  

You should never pass [Global](code-reference/global.md) object directly as argument. Instead just specify that remote callback requires this argument.

Such behavior is helpful when you consider to build [pipeline](pipeline.md) architecture.
