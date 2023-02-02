import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields, replace
from typing import Optional, Tuple, Dict, List, ClassVar, Set, Generator, Union, Any, AsyncIterable, NoReturn

import dill

from daffi.exceptions import RemoteError
from daffi.utils.misc import uuid, Period
from daffi.utils.settings import BYTES_CHUNK, BYTES_LIMIT
from daffi.components.proto import messager_pb2
from daffi.exceptions import InitializationError


logger = logging.getLogger(__name__)


class MessageFlag:
    EMPTY = 0
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
    STOP_REQUEST = 11
    INIT_STREAM = 12
    STREAM_ERROR = 13
    STREAM_THROTTLE = 14
    RECEIVER_ERROR = 15


class Message(ABC):

    __data_fields__: ClassVar[Set[str]] = {}

    @staticmethod
    async def from_message_iterator(
        message_iterator: AsyncIterable,
    ) -> Generator[Union["RpcMessage", "ServiceMessage"], None, None]:
        merged_message: Union["RpcMessage", "ServiceMessage"] = None

        async for msg_chunk in message_iterator:
            if msg_chunk.HasField("rpc_realm"):

                realm = msg_chunk.rpc_realm
                data = realm.data

                if not merged_message:
                    cls = RpcMessage
                    payload = {f.name: getattr(realm, f.name) for f in fields(cls) if f.name not in cls.__data_fields__}
                    payload["data"] = [data]
                    if realm.HasField("period"):
                        payload["period"] = Period(
                            at_time=realm.period.at_time.value, interval=realm.period.interval.value
                        )
                    else:
                        payload["period"] = None
                    merged_message = cls(**payload)
                else:
                    merged_message.data.append(realm.data)

            else:
                realm = msg_chunk.service_realm
                data = realm.data

                if not merged_message:
                    cls = ServiceMessage
                    payload = {f.name: getattr(realm, f.name) for f in fields(cls) if f.name not in cls.__data_fields__}
                    payload["data"] = [data]
                    payload["_loaded"] = False
                    merged_message = cls(**payload)
                else:
                    merged_message.data.append(realm.data)

            if realm.complete_marker:
                yield merged_message
                merged_message = None

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self) if f.name not in self.__data_fields__}

    def copy(self, **changes):
        return replace(self, **changes)

    @abstractmethod
    def dumps(self):
        ...

    @abstractmethod
    def loads(self):
        ...


@dataclass
class ServiceMessage(Message):
    __data_fields__: ClassVar[Set[str]] = {"_loaded", "data"}

    flag: Optional[MessageFlag] = MessageFlag.EMPTY
    transmitter: Optional[str] = ""
    receiver: Optional[str] = ""
    uuid: Optional[int] = field(default_factory=uuid)

    # Byte content.
    _loaded: Optional[bool] = field(repr=False, default=True)
    data: Optional[List[bytes]] = field(repr=False, default=None)

    def dumps(self):
        payload = self.to_dict()

        if self._loaded:
            self._loaded = False
            raw_data = dill.dumps(self.data)
            data_len = len(raw_data)
            if data_len > BYTES_LIMIT:
                InitializationError(
                    f"Size {data_len} is too big for message. Use messages no more then {BYTES_LIMIT} of size."
                ).fire()

            slices = [slice(i, i + BYTES_CHUNK) for i in range(0, data_len, BYTES_CHUNK)]
            self.data = [raw_data[sl] for sl in slices]

        number_of_chunks = len(self.data)

        return (
            messager_pb2.Message(
                service_realm=messager_pb2.ServiceRealm(
                    **payload,
                    data=fd,
                    complete_marker=ind == number_of_chunks,
                )
            )
            if ind == 1
            else messager_pb2.Message(
                service_realm=messager_pb2.ServiceRealm(
                    uuid=self.uuid,
                    data=fd,
                    complete_marker=ind == number_of_chunks,
                )
            )
            for ind, fd in enumerate(self.data, 1)
        )

    def loads(self):
        if not self._loaded:
            self._loaded = True
            self.data = dill.loads(b"".join(self.data))

    def set_data(self, data: Any):
        self._loaded = True
        self.data = data

    @classmethod
    def build_stream_message(cls, data: Any):
        # Build lightweight message without uuid and other unrelated info.
        for msg_chunk in cls(data=data, uuid=0).dumps():
            yield msg_chunk


@dataclass
class RpcMessage(ServiceMessage):
    __data_fields__: ClassVar[Set[str]] = {"func_args", "func_kwargs", "error", "data", "_loaded", "period"}

    return_result: Optional[bool] = True
    func_name: Optional[str] = ""
    period: Optional[Period] = None
    timeout: Optional[int] = 0

    # Byte content.
    func_args: Optional[Tuple] = field(default_factory=tuple)
    func_kwargs: Optional[Dict] = field(default_factory=dict)
    error: Optional[RemoteError] = None

    def dumps(self) -> Generator:
        payload = self.to_dict()
        if not self.data:
            raw_data = dill.dumps((self.func_args, self.func_kwargs, self.error))
            data_len = len(raw_data)
            slices = [slice(i, i + BYTES_CHUNK) for i in range(0, data_len, BYTES_CHUNK)]
            self.data = [raw_data[sl] for sl in slices]

        number_of_chunks = len(self.data)
        # Return metadata only in first chunk

        period_condition = None
        if self.period:
            period_condition = (
                {"at_time": messager_pb2.AtTimeCondition(value=self.period.at_time)}
                if self.period.at_time
                else {"interval": messager_pb2.IntervalCondition(value=self.period.interval)}
            )
        return (
            messager_pb2.Message(
                rpc_realm=messager_pb2.RpcRealm(
                    **payload,
                    data=fd,
                    period=messager_pb2.Period(**period_condition) if self.period else None,
                    complete_marker=ind == number_of_chunks,
                )
            )
            if ind == 1
            else messager_pb2.Message(
                rpc_realm=messager_pb2.RpcRealm(
                    uuid=self.uuid,
                    data=fd,
                    complete_marker=ind == number_of_chunks,
                )
            )
            for ind, fd in enumerate(self.data, 1)
        )

    def loads(self):
        if self.data is not None:
            self.func_args, self.func_kwargs, self.error = dill.loads(b"".join(self.data))
            self.data = None

    def set_error(self, error: RemoteError) -> NoReturn:
        self.data = None
        self.error = error
