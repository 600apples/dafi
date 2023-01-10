Instead of registering a single remote callback by decorating a function, you can decorate a class.
In this case, all public methods of the class become callbacks.


Lets consider we have Node `node-1` with CallbackGroup registered as callback
```python
from daffi import callback


@callback
class CallbackGroup:
    """All public methods (without '_') become callbacks."""

    def __init__(self):
        self.internal_value = "my secret value"

    def method1(self):
        return f"triggered method1. Internal value: {self.internal_value}"

    def method2(self, *args, **kwargs):
        return f"triggered method2 with arguments: {args}, {kwargs}"

    @classmethod
    def class_method(cls, *args, **kwargs):
        return f"triggered classmethod with arguments: {args}, {kwargs}"

    @staticmethod
    def static_method(*args, **kwargs):
        return f"triggered staticmethod with arguments: {args}, {kwargs}"

    def _private_method(self):
        pass
```

This method of registration has certain limitations.

- Only methods that are class methods and statics methods are visible callbacks by default
as to trigger static or class method there is no neeed to instantiate object of class.
  
Now, If we try to execute `method1` we got `GlobalContextError` error

```python
from daffi import Global, FG

proc_name = "node-2"

g = Global(init_controller=True)

g.wait_process('node-1')

result = g.call.method1() & FG

# ---- traceback ----
# GlobalContextError(
# daffi.exceptions.RemoteCallError:
# Exception while processing function 'method1' on remote executor.
# Instance of 'CallbackGroup' is not initialized yet.
# Create instance or mark method 'method1' as classmethod or staticmethod
```

- second limitation is you can have only one instance of `CallbackGroup` created

Next code will thrown `InitializationError`
```python
cb1 = CallbackGroup()
cb2 = CallbackGroup()

# ---- traceback ----
#  InitializationError(f"Only one callback instance of {class_name!r} should be created.")
#  daffi.exceptions.InitializationError: Only one callback instance of 'CallbackGroup' should be created.
```

- and last limitation is you cannot decorate particular methods of class, only class itself!

!!! note
    You still can use `CallbackGroup` instance as regular class instance after initialization.
    For example next code is valid:
    
    ```python
    cb1 = CallbackGroup()

    cb1.method1()
    cb1.method2("foo", "bar")
    cb1.static_method()
    ```
