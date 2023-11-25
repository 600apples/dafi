from daffi import Service, callback
from inspect import isclass, getmembers, isfunction, ismethod

class MyCustomError(Exception):
    pass


@callback
def my_method(*args, **kwargs):
    print(args, kwargs)
    return 3333


service = Service(host="0.0.0.0", port=5000)
service.start()


@callback
def my_method1(*args, **kwargs):
    return 3333

@callback
def my_method2(*args, **kwargs):
    return 3333



service.add_event_handler(lambda x: print(x))

service.join()