import time
from daffi import Client, SerdeFormat, callback
import os
from threading import Thread

@callback
class TestCallback:


    def my_method(self, *args, **kwargs):
        print(args, kwargs)
        return 22222

    def my_method3(self):
        return 22222

client = Client(host="0.0.0.0", port=5000)

client.add_event_handler(lambda x: print(x))

conn = client.connect()


time.sleep(1000)
