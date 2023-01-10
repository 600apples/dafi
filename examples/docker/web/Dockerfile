FROM python:3.8-alpine

WORKDIR /opt

RUN apk --update --upgrade add --no-cache  gcc musl-dev jpeg-dev zlib-dev libffi-dev cairo-dev pango-dev gdk-pixbuf-dev

RUN python3 -m pip install --upgrade pip

RUN python3 -m pip install Flask==2.2.2

EXPOSE 5000 8888

COPY . .

RUN python3 -m pip install daffi

CMD [ "python3", "-u", "app.py" ]
