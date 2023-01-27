Stream is Daffi's special ability to pass iterables to remote callbacks.
It is very optimized for huge bandwidth.

A stream can be considered as a union of `BROADCAST` and `NO_RETURN` execution modifiers.

For instance we have remote callback which takes one `int` as argument and store this argument to 
global list and we need to pass range of integers from 1 to 100 from another `Node`.

(Of course you can pass full range to callback at once but lets imagine you have unknown range or range that can be infinity generator)

process 1
```python
... # Global object initialization is omitted.

from daffi import callback

sequence_store = []

@callback
def process_sequence(a: int) -> None:
    global sequence_store
    sequence_store.append(a)
```

process 2
```python
from daffi import Global, NO_RETURN, fetcher, __body_unknown__


@fetcher
def process_sequence(a: int) -> None:
    __body_unknown__(a)


g = Global(host='localhost', port=8888)

for i in range(1, 101):
    process_sequence(i) & NO_RETURN
```

Or let's even say we have `process_sequence` callback registered on many nodes and we need to pass
range to all of them:

process 2
```python
from daffi import Global, BROADCAST, fetcher, __body_unknown__


@fetcher
def process_sequence(a: int) -> None:
    __body_unknown__(a)


g = Global(host='localhost', port=8888)

for i in range(1, 101):
    process_sequence(i) & BROADCAST
```

The 2 examples above work as they should, but there is a more concise syntax:

process 2
```python
from daffi import Global, STREAM, fetcher, __body_unknown__


@fetcher
def process_sequence(a: int) -> None:
    __body_unknown__(a)


g = Global(host='localhost', port=8888)

integers_range = range(1, 101)

process_sequence(integers_range) & STREAM
```

Callbacks registered for streams have only one limitation. The should take exactly 1 argument
This argument becomes stream item on each iteration of stream process.

    
!!! note
    You still can pass many arguments using stream of tuples or stream of dictionaries
    
    process 1
    ```python
    ... # Global object initialization is omitted.
    
    from daffi import callback
     

    @callback
    def process_stream(args: tuple) -> None:
        arg1, arg2, arg3 = args
        ...
    ```

    process 2
    ```python
    from daffi import Global, STREAM
    
    g = Global(host='localhost', port=8888)
    
    arguments = [(1, 2, "a"), (3, 4, "b"), (5, 6, "c")]
    
    g.call.process_stream(arguments) & STREAM
    ```

You might be wondering why not just use `BROADCAST` and `NO_RETURN` execution modifiers in the cycle?

Answering briefly `STREAM` is much more optimized for passing big ranges to remote callback whereas 
`BROADCAST`, `NO_RETURN`, `FG`, `BG` modifiers are more optimized for single callback execution


#### Example with opencv camera stream

!!! warning
    This example was tested on Ubuntu22.
    It doens't work as expected on Mac since limitation of using opencv's method `imshow` in threads for this OS.

!!! note
    You need to install [opencv-python](https://pypi.org/project/opencv-python/) for this example

Lest consider more realistic usage of stream. Imagine you have one node that read video stream from camera
and you need to show this stream in two other nodes.

Process 1 is stream generator

process 1
```python
import asyncio
import logging
import cv2
from daffi import Global, STREAM, fetcher, __body_unknown__

logging.basicConfig(level=logging.INFO)

cap = cv2.VideoCapture(0)


@fetcher
async def show_stream(frame: int) -> None:
    """Display the resulting image frame"""
    __body_unknown__(frame)


def frame_iterator():
    """Read frame from camera and send it to all receivers"""
    
    # Percent of original size
    # Images with high resolution might cause stream throttling
    # It depends on your camera. Adjust this value if needed.
    scale_percent = 30
    ret, frame = cap.read()
    while ret:
        ret, frame = cap.read()
        width = int(frame.shape[1] * scale_percent / 100)
        height = int(frame.shape[0] * scale_percent / 100)
        dim = (width, height)
        resized = cv2.resize(frame, dim, interpolation=cv2.INTER_AREA)
        yield resized


async def main():
    with Global(init_controller=True, host="localhost", port=8888) as g:
        
        for proc in ('process2', 'process3'):
            g.wait_process(proc)

        show_stream(frame_iterator()) & STREAM

    cap.release()
    cv2.destroyAllWindows()


if __name__ == "__main__":
    asyncio.run(main())
```
(This script is complete, it should run "as is")


Process 2 and Process 3 are stream consumers

process 2
```python
import cv2
import logging
from daffi import Global, callback

logging.basicConfig(level=logging.INFO)


@callback
async def show_stream(frame: int) -> None:
    """Display the resulting image frame"""
    cv2.imshow("daffi stream consumer 1", frame)
    cv2.waitKey(1)


def main():
    g = Global(process_name='process2', host="localhost", port=8888)
    g.join()


if __name__ == "__main__":
    main()
```
(This script is complete, it should run "as is")


process 3
```python
import cv2
import logging
from daffi import Global, callback

logging.basicConfig(level=logging.INFO)


@callback
async def show_stream(frame: int) -> None:
    """Display the resulting image frame"""
    cv2.imshow("daffi stream consumer 2", frame)
    cv2.waitKey(1)


def main():
    g = Global(process_name='process3', host="localhost", port=8888)
    g.join()


if __name__ == "__main__":
    main()
```
(This script is complete, it should run "as is")


Then in 3 separate terminals:
```bash
python process1.py
python process2.py
python process3.py
```

You should see 2 windows with camera stream on your machine


