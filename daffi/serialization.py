import json
import pickle
from typing import Union, Tuple


class SerdeFormat:
    RAW = 0
    JSON = 1
    PICKLE = 2


class Serializer:

    """
    A class that provides serialization and deserialization of objects.
    """

    @staticmethod
    def serialize(
        serde: SerdeFormat, *args, **kwargs
    ) -> Tuple[Union[bytes, str], bool]:
        """
        Serialize rpc arguments.
        """
        if serde == SerdeFormat.RAW:
            # Take the first argument from args or kwargs. Other arguments are ignored.
            if args:
                data = args[0]
            elif kwargs:
                data = kwargs[next(iter(kwargs))]
            else:
                data = b""
            is_bytes = isinstance(data, bytes)
        elif serde == SerdeFormat.JSON:
            data = json.dumps({"args": args, "kwargs": kwargs})
            is_bytes = False
        elif serde == SerdeFormat.PICKLE:
            data = pickle.dumps((args, kwargs))
            is_bytes = True
        else:
            raise ValueError(f"Unknown serde format: {serde}")
        return data, is_bytes

    @staticmethod
    def deserialize(serde: SerdeFormat, data: Union[bytes, str]):
        """
        Deserialize rpc arguments.
        """
        if serde == SerdeFormat.RAW:
            return (data,), {}
        elif serde == SerdeFormat.JSON:
            data = json.loads(data)
            return data["args"], data["kwargs"]
        elif serde == SerdeFormat.PICKLE:
            return pickle.loads(data)
        else:
            raise ValueError(f"Unknown serde format: {serde}")
