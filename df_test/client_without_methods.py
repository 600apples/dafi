import time
from daffi import Client, SerdeFormat
from daffi.core import send_message_from_client, MessageFlag
import os
from threading import Thread


client = Client(host="0.0.0.0", port=5000)
conn = client.connect()

total = time.time()
for i in range(100000):
    start = time.time()
    data = conn.rpc(serde=SerdeFormat.PICKLE, timeout=4).my_method()

        # print(len(data.results))




    # print(f"finished in {time.time() - start} seconds")
print(f"total items {i}")
print(f"total time: {time.time() - total} seconds")

time.sleep(0.1)


# while True:
#     res = send_message_from_client(
#                     data="",
#                     flag=MessageFlag.HANDSHAKE,
#                     serde=SerdeFormat.RAW,
#                     receiver="",
#                     func_name="",
#                     return_result=True,
#                     conn_num=0,
#                     is_bytes=False,
#                 )
#
#     print(res)
#     time.sleep(10)

print(data)
