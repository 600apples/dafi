import os
import logging
from daffi import Global, FG

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DAFI_PROCESS_NAME = "web"
DAFI_HOST = os.environ["DAFI_HOST"]
DAFI_PORT = os.environ["DAFI_PORT"]
DAFI_INIT_CONTROLLER = DAFI_HOST == DAFI_PROCESS_NAME

g = Global(
    process_name=DAFI_PROCESS_NAME,
    host=DAFI_HOST,
    port=DAFI_PORT,
    init_controller=DAFI_INIT_CONTROLLER,
)

app = Flask(__name__)
app.config["SECRET_KEY"] = "your secret key"

messages = [{"title": "Message One", "content": "Message One Content", "color": "black"}]


@app.route("/", methods=("GET", "POST"))
def index():
    if request.method == "POST":
        title = request.form["title"]
        content = request.form["content"]

        if not title:
            flash("Title is required!")
        elif not content:
            flash("Content is required!")
        else:

            logger.warning(f"Calling a remote callback to take message color...")
            color = g.call.colorize(title=title, content=content) & FG
            logger.warning(f"Received {color!r} color from remote callback")

            messages.append({"title": title, "content": content, "color": color})
            return redirect(url_for("index"))

    return render_template("index.html", messages=messages)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False)
