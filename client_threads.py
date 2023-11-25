import simple
from threading import Thread
import pickle
import ctypes

import time

args = [1,2, 3, 4, 5, 6, 7, 8, 9, 10]
kwargs = {"a":1, "b":2, "c":3, "d":4, "e":5}
intotal = (args, kwargs)

i = [0]

uuids = []

simple.startClient("127.0.0.1", 5000, 1)


def send_message(data: bytes, flag: int, transmitter: str, receiver: str, func_name: str, timeout: int, return_result: bool, metadata_context: str) -> int:
    data = ctypes.create_string_buffer(data, len(data))
    return simple.sendMessage(data, flag, transmitter, receiver, func_name, timeout, return_result, metadata_context)


def send_messages():
    msgcount = 0

    for i in range(1000):
        data = pickle.dumps(intotal, protocol=5)
        uuid, ts = send_message(data, 0, "client", "server", "func", 0, True, "test")
        data, flag, transmitter, receiver, func_name, return_result = simple.getMessageFromClientStore(uuid, ts + 40)


        uuids.append(uuid)

        # print(f"after send: {data}")
        restored = pickle.loads(data)
        print(f"restored: {restored}")
        msgcount += 1

    print("message sent = ", msgcount)
    if msgcount != 1000:
        raise ValueError("msgcount != 1000")


    # print("message sent")
    # print("time taken: ", time.time() - start)


if __name__ == '__main__':
    start = time.time()
    threads = [Thread(target=send_messages) for i in range(100)]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print("client stopped")
    print("TOTAL time taken: ", time.time() - start)
    print("total uuids: ", len(uuids))




