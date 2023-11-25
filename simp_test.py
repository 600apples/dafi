import dfcore
import asyncio


async def run_server():
    conn_num = dfcore.startServer("127.0.0.1", 5000, 0, "")
    print(f"server started, connection number: {conn_num}")
    dfcore.detachServer(conn_num)

async def simple_counter():
    while True:
        await asyncio.sleep(4)
        # print("counter")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(run_server(), simple_counter()))
    loop.close()

#
# a = input("enter a: ")
# b = input("enter b: ")
# print("sum: ",simple.sum(int(a),int(b)))
# print("multiple: ",simple.mul(int(a),int(b)))
# print("type sum: ", type(simple.sum(int(a),int(b))))
# simple.hello()
# simple.printSt(input("enter something: "))
# print(simple.returnArrayWithInput(100))