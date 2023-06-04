#!/bin/bash
# Generate self-signed certificates

set -e

IP="::"

openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
  -keyout key.pem -out cert.pem -subj "/CN=example.com" \
  -addext "subjectAltName=DNS:example.com,DNS:www.example.net,IP:${IP}"
