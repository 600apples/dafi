import struct
import logging
from dataclasses import dataclass, field, fields
from enum import IntEnum
from typing import Optional, Tuple, Dict, List

import dill

from dafi.exceptions import RemoteError, InitializationError
from dafi.utils.misc import uuid as msg_uuid, Period

BYTES_CHUNK = 1024  # 15 Kb
BYTES_LIMIT = 3e6  # 3 Mb

logger = logging.getLogger(__name__)


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
    BROADCAST = 10


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

    def dumps(self) -> List[bytes]:
        payload = tuple(map(lambda f: getattr(self, f.name), fields(self)))
        msg = dill.dumps(payload)
        msg_len = len(msg)
        if msg_len > BYTES_LIMIT:
            err = "Payload is too big! 3 Mb is maximum allowed payload."
            logger.error(err)
            raise InitializationError(err)

        len_marker = struct.pack(">I", len(msg))
        chunked_payload = [
            len_marker + msg[i : i + BYTES_CHUNK] if i == 0 else msg[i : i + BYTES_CHUNK]
            for i in range(0, len(msg), BYTES_CHUNK)
        ]
        return chunked_payload

    @classmethod
    async def loads(cls, stream, raw_msglen) -> "Message":
        msglen = cls.msglen(raw_msglen)
        payload = await cls.recvall(stream, msglen)
        if payload:
            data = dill.loads(payload)
            return cls(*data)

    @staticmethod
    def msglen(raw: bytes) -> int:
        return struct.unpack(">I", raw)[0]

    @classmethod
    async def recvall(cls, stream, total_len):
        # Helper function to recv n bytes or return None if EOF is hit

        data = bytearray()
        full_chunks, rest = divmod(total_len, BYTES_CHUNK)
        for chunk in [BYTES_CHUNK] * full_chunks + [rest] if rest else []:
            packet = await stream.receive(chunk)
            if not packet:
                return None
            data.extend(packet)
        return data

    def swap_applicants(self):
        if self.receiver is None or self.transmitter is None:
            raise ValueError("Both receiver and transmitter should be not empty to swap.")
        self.receiver, self.transmitter = self.transmitter, self.receiver
