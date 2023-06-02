Communication between daffi applications can be protected with SSL certificate and key.

For this example consider to use following script in order to generate self signed certificates for localhost IPV6

```bash
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

rm -f domains.ext root_ca.pem root_key.pem sign.csr
```

As the result you should see `cert.pem` and `key.pem` files created in directory where script was executed.


=== "class based approach"

    `calculator_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    class CalculatorService(Callback):
        auto_init = True
    
        def calculate_sum(self, *numbers):
            return sum(numbers)
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888, ssl_certificate="cert.pem", ssl_key="key.pem").join()
    ```
    
    `calculator_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.registry import Fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    class CalculatorClient(Fetcher):
    
        def calculate_sum(self, *numbers):
            """
            Note: functions without a body are treated as proxies for remote callbacks.
            All arguments provided to this function will be sent to the remote service as-is.
            """
            pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888, ssl_certificate="cert.pem", ssl_key="key.pem")
    
        calc_client = CalculatorClient()
        result = calc_client.calculate_sum(1, 2)
        print(result)
    
        result = calc_client.calculate_sum(10, 20, 30)
        print(result)
    
        g.stop()
    ```
    
    Execute in two separate terminals:
    ```bash
    python3 calculator_service.py
    python3 calculator_client.py
    ```

=== "decorator based approach"

    `calculator_service.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import callback
    
    logging.basicConfig(level=logging.INFO)
    
    
    @callback
    def calculate_sum(*numbers):
        return sum(numbers)
    
    
    if __name__ == '__main__':
        Global(init_controller=True, host="localhost", port=8888, ssl_certificate="cert.pem", ssl_key="key.pem").join()
    ```
    
    `calculator_client.py` content:
    ```python
    import logging
    from daffi import Global
    from daffi.decorators import fetcher
    
    logging.basicConfig(level=logging.INFO)
    
    
    @fetcher
    def calculate_sum(*numbers):
        """
        Note: functions without a body are treated as proxies for remote callbacks.
        All arguments provided to this function will be sent to the remote service as-is.
        """
        pass
    
    
    if __name__ == '__main__':
        g = Global(host="localhost", port=8888, ssl_certificate="cert.pem", ssl_key="key.pem")
    
        result = calculate_sum(1, 2)
        print(result)
    
        result = calculate_sum(10, 20, 30)
        print(result)
    
        g.stop()
    ```
    
    Execute in two separate terminals:
    ```bash
    python3 calculator_service.py
    python3 calculator_client.py
    ```


!!! note 
    To use UNIX socket instead of TCP for communication, you should remove the `host` and `port` parameters from 
    the initialization of the Global object, and optionally include the `unix_sock_path` parameter.
    