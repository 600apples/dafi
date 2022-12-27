import sys
from dafi import Global, callback

email = "noboby@gmail.com"

@callback
def send_email(title: str, content: str):
    print(f"Email has been sent:\nemail title = {title}, email content = {content}")


if __name__ == '__main__':
    g = Global(host="localhost", port=8888, init_controller=True, process_name="Data sender")
    g.join()

