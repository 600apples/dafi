import time
from daffi.application import Application

app1 = Application(host="127.0.0.1", port=5000)
app1.start_as_service()


app2 = Application(host="127.0.0.1", port=5555)
app2.start_as_service()

print('before join')
app1.join()
print("app1 joined")

time.sleep(100)

