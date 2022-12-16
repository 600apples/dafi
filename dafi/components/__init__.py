from enum import IntEnum
from threading import Event
from dafi.backend import BackEnd
from dafi.interface import Master, Node


class MasterStatus(IntEnum):

    RUNNING = 1
    UNAVAILABLE = 2


class ComponentsBase(Master, Node):
    TIMEOUT = 10 # 10 sec

    def __init__(self, process_name: str, backend: BackEnd, stop_event: Event):
        self.backend = backend
        self.process_name = process_name
        self.stop_event = stop_event
