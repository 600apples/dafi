from itertools import cycle
from dafi import Global, callback, NO_RETURN

colors = cycle(("red", "green", "blue"))


@callback
async def colorize(title: str, content: str, g: Global):
    color = next(colors)
    g.call.send_email(title=title, content=content) & NO_RETURN
    return color


if __name__ == '__main__':
    g = Global(host="localhost", port=8888, process_name="Colorizer")
    g.join()

