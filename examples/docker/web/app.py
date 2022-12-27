from dafi import Global, FG

from flask import(
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
)

g = Global(init_controller=True, host="localhost", port=8888)

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

            color = g.call.colorize(title=title, content=content) & FG

            messages.append({"title": title, "content": content, "color": color})
            return redirect(url_for("index"))

    return render_template("index.html", messages=messages)



if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)