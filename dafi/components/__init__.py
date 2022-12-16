import os
from enum import IntEnum
from functools import cached_property
from threading import Event as ThEvent

from dafi.exceptions import InitializationError
from dafi.interface import ControllerI, NodeI, BackEndI


class ControllerStatus(IntEnum):

    RUNNING = 1
    UNAVAILABLE = 2


class ComponentsBase(ControllerI, NodeI):
    TIMEOUT = 6  # 6 sec

    def __init__(self, process_name: str, backend: BackEndI, stop_event: ThEvent):
        self.backend = backend
        self.process_name = process_name
        self.stop_event = stop_event


class UnixComponentBase(ComponentsBase):
    SOCK_FILE = ".sock"

    @cached_property
    def socket(self):
        base_dir = self.backend.base_dir
        if not os.path.exists(base_dir):
            raise InitializationError(
                f"Backend {self.backend} is not suitable for creating unix connections. "
                f"Property 'base_dir' should return the path to a local directory that exists"
            )
        return os.path.join(base_dir, self.SOCK_FILE)
