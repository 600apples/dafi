Global object takes 3 lifecycle callbacks

-  `on_init` executed once Node, Controller or Controller and Node are initialized. It is one time event
- `on_node_connect` - callback is triggered when the Node establishes a connection with the Controller and can occur multiple times, including instances where re-connections are involved
- `on_node_disconnect` -  callback is triggered when the Node is disconnected from Controller and can occur multiple times, including instances where re-connections are involved

```python
import logging
from daffi import Global

logging.basicConfig(level=logging.INFO)


def on_init(g: Global, process_name: str):
    print("On init")


def on_node_connect(g: Global, process_name: str):
    print("Node connected")


def on_node_disconnect(g: Global, process_name: str):
    print("Node disconnected")


if __name__ == "__main__":
    Global(
        init_controller=True,
        host="localhost",
        port=8888,
        on_init=on_init,
        on_node_connect=on_node_connect,
        on_node_disconnect=on_node_disconnect,
    ).join()
```