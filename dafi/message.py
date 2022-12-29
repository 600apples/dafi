import struct
from dataclasses import dataclass, field, fields
from enum import IntEnum
from typing import Optional, Tuple, Dict

import dill

from dafi.exceptions import RemoteError
from dafi.utils.misc import uuid as msg_uuid, Period


class MessageFlag(IntEnum):
    HANDSHAKE = 1
    REQUEST = 2
    SUCCESS = 3
    UPDATE_CALLBACKS = 4
    UNABLE_TO_FIND_CANDIDATE = 5
    UNABLE_TO_FIND_PROCESS = 6
    REMOTE_STOPPED_UNEXPECTEDLY = 7
    SCHEDULER_ERROR = 8
    SCHEDULER_ACCEPT = 9


@dataclass
class Message:

    flag: MessageFlag
    transmitter: str
    receiver: Optional[str] = None
    uuid: Optional[str] = field(default_factory=msg_uuid)
    func_name: Optional[str] = None
    func_args: Optional[Tuple] = field(default_factory=tuple)
    func_kwargs: Optional[Dict] = field(default_factory=dict)
    return_result: Optional[bool] = True
    error: Optional[RemoteError] = None
    period: Optional[Period] = None

    def dumps(self) -> bytes:
        payload = tuple(map(lambda f: getattr(self, f.name), fields(self)))
        msg = dill.dumps(payload)
        return struct.pack(">I", len(msg)) + msg

    @classmethod
    def loads(cls, payload: bytes) -> "Message":
        data = dill.loads(payload)
        return cls(*data)

    @staticmethod
    def msglen(raw: bytes) -> int:
        return struct.unpack(">I", raw)[0]

    def swap_applicants(self):
        if self.receiver is None or self.transmitter is None:
            raise ValueError("Both receiver and transmitter should be not empty to swap.")
        self.receiver, self.transmitter = self.transmitter, self.receiver
