FROM python:3.9-alpine

WORKDIR /opt

RUN apk add --no-cache linux-headers build-base

RUN python3 -m pip install --upgrade pip

COPY main.py .

RUN python3 -m pip install daffi

CMD [ "python3", "-u", "main.py" ]
