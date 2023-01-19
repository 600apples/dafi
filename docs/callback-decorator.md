
[callback](code-reference/callback.md) decorator registers function or class as remote callback. This way function (or all publicly available methods of decorated class) become visible for all other nodes by its name.
[callback](code-reference/callback.md) decorator can register both synchronous and asynchronous functions/methods

```python
from daffi import callback

@callback
def sum_two_numbers(a: int, b: int) -> int:
    return a + b


@callback
async def subtract_two_numbers(a: int, b: int) -> int:
    return a - b
```

in example above we registered callbacks with names `sum_two_numbers` and `subtract_two_numbers`.
Now it is possible to execute callback from remote process:

```python
from daffi import Global, FG

g = Global(process_name='my awersome process', init_controller=True, host='localhost', port=8888)

result1 = g.call.sum_two_numbers(10, 15) & FG
result2 = g.call.subtract_two_numbers(22, 13) & FG
```

`FG` in this example is execution modifier. You can read about execution modifiers [here](execution-modifiers.md)
 
!!! warning
    Do not use the same callback names in different processes unless you want to use `BROADCAST` or `STREAM` execution modifiers.
    
    For singular execution, for instance if you have `sum_two_numbers` callback registered in process `A` and in process `B` then
    only one of them will be triggered. Daffi use random strategy to execute callback by name. You cannot control which one
    will be triggered.
    
!!! note
    You still can use registered [callback](code-reference/callback.md) as regular python function.
    Next example works fine locally:
    
    ```python
    from daffi import callback
    
    @callback
    def my_callback(a, b, c) -> int:
        print(a, b, c)
        
    my_callback(1, 2, 3)
    ```


instead of decorating functions you can create a decorated class:

```python
@callback
class RemoteCallbackGroup:

    def method1(self):
        ...
    
    def method2(self):
        ...
    
    @staticmethod
    def method3():
        ...
```   

In this case all public methods of decorated class become remote callbacks (`public` means without underscore in the beginning of name)

Class callback group initialization has some limitations. Read [this](callback-classes.md) article to know more.
