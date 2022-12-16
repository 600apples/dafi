import string
from random import choices
import sniffio
from typing import TypeVar, Dict

K = TypeVar("K", bound=str)
GlobalCallback = TypeVar("GlobalCallback")

CALLBACK_MAPPING: Dict[K, GlobalCallback] = dict()
CALLBACKS_WITH_CUSTOM_NAMES = set()

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def async_library():
    try:
        asynclib_name = sniffio.current_async_library()
    except sniffio.AsyncLibraryNotFoundError:
        asynclib_name = "asyncio"
    return asynclib_name


def uuid(k: int = 8):
    alphabet = string.ascii_lowercase + string.digits
    return "".join(choices(alphabet, k=k))
