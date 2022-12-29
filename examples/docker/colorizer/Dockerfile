FROM python:3.8-alpine

WORKDIR /opt

RUN apk --update --upgrade add --no-cache  gcc musl-dev jpeg-dev zlib-dev libffi-dev cairo-dev pango-dev gdk-pixbuf-dev

RUN python3 -m pip install --upgrade pip


EXPOSE 8888

COPY . .

#RUN python3 -m pip install dafi-0.0.1-py3-none-any.whl # TODO release package

CMD [ "python3", "-u", "main.py" ]
