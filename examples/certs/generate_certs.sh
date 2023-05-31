#!/bin/bash
# Generate self-signed certificates

set -e

IP="::"

cat << EOF > domains.ext
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = IP:$IP
EOF

# Root CA certificate and key
openssl req -x509 -nodes -new -sha256 -days 3650 -newkey rsa:2048 -keyout root_key.pem -out root_ca.pem -subj "/CN=Root-CA"
# Generate private key and CSR
openssl req -new -nodes -newkey rsa:2048 -keyout key.pem -out sign.csr -subj "/C=NA/ST=NA/L=NA/O=ORG/CN=example.com"
# Generate domain certificate
openssl x509 -req -in sign.csr -CA root_ca.pem -CAkey root_key.pem -CAcreateserial -out cert.pem -days 3650 -sha256 -extfile domains.ext
