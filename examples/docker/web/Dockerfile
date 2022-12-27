FROM python:3.8-alpine

WORKDIR /opt

RUN apk --update --upgrade add --no-cache  gcc musl-dev jpeg-dev zlib-dev libffi-dev cairo-dev pango-dev gdk-pixbuf-dev

RUN python -m pip install --upgrade pip

RUN python -m pip install Flask==2.2.2

EXPOSE 7007

COPY . .

CMD [ "python", "app.py" ]
