from daffi import CallbackGroup, callback


from daffi.registry._callback import LOCAL_CALLBACK_MAPPING


class TestCallback(CallbackGroup):


    def my_method(self):
        pass

    def my_method3(self):
        pass

    async def method5(self):
        pass

@callback
def my_method2(self):
    pass


print(LOCAL_CALLBACK_MAPPING)