import simple
from threading import Thread
import time


simple.initThreads()

def send_messages():
    simple.DoSome()

    # while True:
    #     data = "hello world"
    #     simple.sendMessage(data)
    #     time.sleep(5)


def reported():
    while True:
        print("message sent")
        time.sleep(5)



def main():
    t1 = Thread(target=send_messages)
    t2 = Thread(target=reported)
    t2.start()
    time.sleep(1)

    t1.start()
    t1.join()
    t2.join()


if __name__ == '__main__':
    main()