import struct
from enum import IntEnum
from typing import Optional, Tuple, Dict
from dafi.utils import uuid as msg_uuid

import dill

class MessageFlag(IntEnum):
    HANDSHAKE = 1
    REQUEST = 2
    EXECUTE = 3


class Message:

    def __init__(
            self,
            flag: MessageFlag,
            transmitter: str,
            receiver: Optional[str] = None,
            func_name: Optional[str] = None,
            args: Optional[Tuple] = None,
            kwargs: Optional[Dict] = None,
            uuid: Optional[str] = None,
    ):
        self.flag = flag
        self.transmitter = transmitter
        self.receiver = receiver,
        self.func_name = func_name
        self.args = args or tuple()
        self.kwargs = kwargs or dict()
        self.uuid = uuid or msg_uuid()

    def dumps(self):
        payload = (self.flag, self.transmitter, self.receiver, self.func_name, self.args, self.kwargs, self.uuid)
        msg = dill.dumps(payload)
        return struct.pack('>I', len(msg)) + msg

    @classmethod
    def loads(cls, payload: bytes) -> "Message":
        data = dill.loads(payload)
        return cls(*data)

    @staticmethod
    def msglen(raw: bytes) -> int:
        return struct.unpack('>I', raw)[0]
