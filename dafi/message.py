import logging
from dataclasses import dataclass, field, fields
from typing import Optional, Tuple, Dict, List

import dill

from dafi.exceptions import RemoteError, InitializationError
from dafi.utils.misc import uuid, Period
from dafi.utils.settings import BYTES_CHUNK, BYTES_LIMIT


logger = logging.getLogger(__name__)


class MessageFlag:
    HANDSHAKE = b"\x01"
    REQUEST = b"\x02"
    SUCCESS = b"\x03"
    UPDATE_CALLBACKS = b"\x04"
    UNABLE_TO_FIND_CANDIDATE = b"\x05"
    UNABLE_TO_FIND_PROCESS = b"\x06"
    REMOTE_STOPPED_UNEXPECTEDLY = b"\x07"
    SCHEDULER_ERROR = b"\x08"
    SCHEDULER_ACCEPT = b"\x09"
    BROADCAST = b"\x10"


@dataclass
class Message:

    flag: MessageFlag
    transmitter: str
    receiver: Optional[str] = None
    uuid: Optional[int] = field(default_factory=uuid)
    func_name: Optional[str] = None
    func_args: Optional[Tuple] = field(default_factory=tuple)
    func_kwargs: Optional[Dict] = field(default_factory=dict)
    return_result: Optional[bool] = True
    error: Optional[RemoteError] = None
    period: Optional[Period] = None

    @property
    def str_uuid(self):
        return str(self.uuid)

    def dumps(self) -> List[bytes]:
        payload = tuple(map(lambda f: getattr(self, f.name), fields(self)))
        msg = dill.dumps(payload)
        msg_len = len(msg)
        if msg_len > BYTES_LIMIT:
            err = "Payload is too big! 3 Mb is maximum allowed payload."
            logger.error(err)
            raise InitializationError(err)

        msg_marker = ((self.uuid << 32) | msg_len).to_bytes(8, "big")
        slices = [slice(i, i + BYTES_CHUNK) for i in range(0, msg_len, BYTES_CHUNK)]
        chunked_payload = [msg_marker + msg[sl] if ind == 1 else msg[sl] for ind, sl in enumerate(slices, 1)]
        return chunked_payload

    def swap_applicants(self):
        if self.receiver is None or self.transmitter is None:
            raise ValueError("Both receiver and transmitter should be not empty to swap.")
        self.receiver, self.transmitter = self.transmitter, self.receiver

    @staticmethod
    def get_msg_uuid_and_len_from_bytes(data: bytes) -> Tuple[int, int]:
        restored_msg_marker = int.from_bytes(data, "big")
        msg_uuid = (restored_msg_marker & 0xFFFFFFFF00000000) >> 32
        msg_len = restored_msg_marker & 0x00000000FFFFFFFF
        return msg_uuid, msg_len
