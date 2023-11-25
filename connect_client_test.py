import dfcore
import gc
from threading import Thread
import pickle
import asyncio
import ctypes
import numpy as np

import time

args = [1,2, 3, 4, 5, 6, 7, 8, 9, 10]
kwargs = {"a":1, "b":2, "c":3, "d":4, "e":5}
intotal = (args, kwargs)

class T:
    pass

t = T()

app_name = "test_app"
password = "1111"
dfcore.startClient("127.0.0.1", 5000, password, app_name)

dfcore.registerMethod("foobar")
dfcore.registerMethod("barbiz")
dfcore.registerMethod("foobar2")



def send_message_bytes(data: bytes, flag: int, decoder: int, receivers: str, func_name: str, timeout: int, return_result: bool, metadata_context: str) -> int:
    data = ctypes.create_string_buffer(data, len(data))
    msg = dfcore.sendMessage(data, flag, decoder, True, receivers, func_name, timeout, return_result, metadata_context)
    return msg

def send_message_string(data: str, flag: int, decoder: int, receivers: str, func_name: str, timeout: int, return_result: bool, metadata_context: str) -> int:
    data = ctypes.create_string_buffer(data.encode(), len(data))
    msg = dfcore.sendMessage(data, flag, decoder, False, receivers, func_name, timeout, return_result, metadata_context)
    return msg


uuid, ts = send_message_bytes(b"foobar", 0, 1, "server", "func", 0, True, "")
# encode
# data = pickle.dumps(intotal, protocol=5)
# uuid, ts = send_message(data, 0, "client", "server", "func", 0, True, "test")


# print(f"original: {data}")

# data, flag, transmitter, receiver, func_name, return_result = dfcore.getMessageFromClientStore(uuid, ts + 40)


# print(f"after send: {data}")
# restored_data = pickle.loads(data)
# print(f"restored: {restored_data}")

#
# arr = np.random.default_rng(1991).random((10, 10, 10))
# #
# #
# #
# #
# arr = pickle.dumps(arr, protocol=5)
#
# print(len(arr))
#
# stingarr = "1" * 800
#
# msgcount = 0
# async def send_messages():
#     global msgcount
#
#     while True:
#
#         await asyncio.sleep(0.001)
#         if msgcount % 10000 == 0:
#             print("some braik")
#             await asyncio.sleep(5)
#
#         data = pickle.dumps(intotal, protocol=5)
#         uuid, ts = send_message_bytes(data, 0, 1, "server", "func", 0, True, "")
#         recvdata, flag, decoder, transmitter, receiver, func_name, return_result = dfcore.getMessageFromClientStore(uuid, ts + 40)
#         restored_data = pickle.loads(recvdata)
#         # print(restored_data)
#
#         # print("decoded data = ", recvdata)
#         print("return_result = ", return_result)
#         # print("decoder = ", decoder)
#         # print("return_result = ", return_result)
#
#         msgcount += 1
#
#
#     print("message sent")
#     print("time taken: ", time.time() - start)
#
#
# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()
#     group1 = asyncio.gather(*[send_messages() for i in range(1, 2)])
#
#
#     all_groups = asyncio.gather(group1)
#
#     start = time.time()
#     results = loop.run_until_complete(all_groups)
#
#     loop.close()
#     tt = time.time() - start
#     print("client stopped")
#     print("TOTAL time taken: ", tt)
#     print("total messages: ", msgcount)
#
#
# time.sleep(200000)