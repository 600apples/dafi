1.3.0
=====
Date: 2023-01-19

-  Added `STREAM` execution modifier
- `daffi.globals.Global`
    - Set default `reconnect_freq` = 30
    - Added `wait_process_async` and `wait_function_async` methods
- `daffi/utils/settings.py`
    - Set `BYTES_CHUNK` = 4 megabytes


1.4.0
=====
Date: 2023-01-23

- Added `fetcher` and `callback_and_fetcher` decorators
- Added `retry_policy` argument to FG and BROADCAST execution modifiers
