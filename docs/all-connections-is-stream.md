All daffi connections are [grpc](https://grpc.io/docs/) http2 streams.<br>
In general, this is good because duffi nodes exchange information without additional connection establishments and other associated delays.

But if your applications are managed by proxy servers like nginx or others then these proxies can impose limits on the duration of the connection.

If it is your case please consider option to increase timeouts.

You can find how to do it for nginx [here](https://easyengine.io/tutorials/php/increase-script-execution-time/)
