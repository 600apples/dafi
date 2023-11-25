import simple
import time
import asyncio

simple.initThreads()

async def send_messages():
    simple.DoSome()



async def reported():
    while True:
        print("message sent")
        await asyncio.sleep(5)



async def main():
    await asyncio.gather(send_messages(), reported())


if __name__ == '__main__':
    asyncio.run(main())