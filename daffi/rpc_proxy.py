import json
import time
import logging
from itertools import repeat
from typing import Union, Tuple, List, Optional
from contextlib import contextmanager
from daffi.serialization import SerdeFormat, Serializer
from daffi.utils.misc import iterable
from daffi.misc import calc_sleep_and_backoff
from daffi.registry.executor import EXECUTOR_REGISTRY
from daffi.core import send_message_from_client, get_message_from_client_store, mark_message_as_expired, set_service_methods, send_handshake_from_client, get_available_members, MessageFlag


METADATA_SEPARATOR = ","


class TransmissionFailure(Exception):
    ...


class RemoteCallError(Exception):
    ...


@contextmanager
def system_exception_handler(msg_template: str, errtype: type):
    try:
        yield
    except SystemError as e:
        origin_err_name = e.__cause__.args[0]
        raise errtype(msg_template.format(origin_err_name)).with_traceback(None) from None


class RpcProxy:

    def __init__(
            self,
            conn: "ClientConnection",
            timeout: Union[int, None],
            receiver: Union[str, List[str], None],
            serde: SerdeFormat,
            return_result: bool,
            logger: logging.Logger,
    ):
        self.conn = conn
        self.timeout = int(timeout or 0)
        self.receiver = METADATA_SEPARATOR.join(receiver) if iterable(receiver) else receiver or ""
        self.serde = serde
        self.return_result = return_result
        self.logger = logger
        self._func_name = None
        self._receiver = {receiver} if receiver else set()
        self._uuid = None

    def __str__(self):
        req_name = "rpc call" if self.return_result else "stream"
        to_receiver = f" to {self.receiver}" if self.receiver else ""
        details = f"(fn: {self._func_name!r}, uuid: {self._uuid})" if self._uuid else f"(fn: {self._func_name!r})"
        return f"{req_name}{to_receiver}{details}"

    __repr__ = __str__

    def __call__(self, *args, **kwargs) -> Tuple[int, int]:
        if self.return_result:
            return self._process_rpc(*args, **kwargs)
        else:
            self._process_stream(*args, **kwargs)

    def __getattr__(self, item):
        self._func_name = item
        return self

    def _process_rpc(self, *args, **kwargs):
        conn_num = self.conn.client._conn_num
        data, is_bytes = Serializer.serialize(self.serde, *args, **kwargs)
        assert self._func_name is not None
        with system_exception_handler(f"Unable to proceed with {self}: {{}}", TransmissionFailure):
            uuid, ts, found_receiver = send_message_from_client(
                data=data,
                flag=MessageFlag.REQUEST,
                serde=self.serde,
                receiver=self.receiver,
                func_name=self._func_name,
                return_result=self.return_result,
                conn_num=conn_num,
                is_bytes=is_bytes,
            )
        self._uuid = uuid
        found_receivers = None if not found_receiver else set(found_receiver.split(METADATA_SEPARATOR))
        if not found_receivers:
            members = "" if not (_memebers := get_available_members(conn_num)) else f"\nAvailable receivers: {_memebers}"
            raise TransmissionFailure(f"No receivers found for {self}." + members)
        elif missing_receivers := self._receiver - found_receivers:
            self.logger.warning(f"Receiver(s): {missing_receivers} seems to be offline.")
        result = RpcResult(conn_num=conn_num, uuid=uuid, ts=ts, timeout=self.timeout, receivers=found_receivers, proxy=self)
        data, flag, serde = result.result()
        return Serializer.deserialize(serde, data)[0][0]

    def _process_stream(self, *args, **kwargs):
        conn_num = self.conn.client._conn_num
        assert self._func_name is not None
        if args:
            data = args[0]
        elif kwargs:
            data = kwargs[next(iter(kwargs))]
        else:
            data = None
        if not iterable(data):
            items = [(args, kwargs)]
        else:
            items = iter(zip(data, repeat({})))

        for (a, k) in items:
            data, is_bytes = Serializer.serialize(self.serde, *a, **k)
            with system_exception_handler(f"Unable to proceed with {self}: {{}}", TransmissionFailure):
                uuid, ts, found_receiver = send_message_from_client(
                    data=data,
                    flag=MessageFlag.REQUEST,
                    serde=self.serde,
                    receiver=self.receiver,
                    func_name=self._func_name,
                    return_result=self.return_result,
                    conn_num=conn_num,
                    is_bytes=is_bytes,
                )
            if not found_receiver:  # empty string
                members = "" if not (_memebers := get_available_members(conn_num)) else f"\nAvailable receivers: {_memebers}"
                raise TransmissionFailure(f"No receivers found for {self}." + members)
            elif missing_receivers := self._receiver - set(found_receiver.split(METADATA_SEPARATOR)):
                self.logger.warning(f"Receiver(s): {missing_receivers} seems to be offline.")

    @classmethod
    def _process_client_handshake(cls, conn_num: int):
        with system_exception_handler("unable to establish handshake: {}", TransmissionFailure):
            data = METADATA_SEPARATOR.join([name for name, _ in EXECUTOR_REGISTRY])
            uuid, ts, found_receiver = send_handshake_from_client(
                password="",
                methods=data,
                conn_num=conn_num,
            )
            try:
                data, _, _ = RpcResult(conn_num=conn_num, uuid=uuid, ts=ts, timeout=5, receivers=None, proxy=None).result()
                return json.loads(data)
            except TimeoutError:
                raise TransmissionFailure("Handshake failed due to timeout.")

    @classmethod
    def _process_service_handshake(cls, conn_num: int):
        data = METADATA_SEPARATOR.join([name for name, _ in EXECUTOR_REGISTRY])
        set_service_methods(data, conn_num)


class RpcResult:

    def __init__(self, conn_num: int, uuid: int, ts: int, timeout: int, receivers: Optional[set[str]] = None, proxy: Optional[RpcProxy] = None):
        self.conn_num = conn_num
        self.uuid = uuid
        self.send_ts = ts
        self.timeout = timeout
        self.receivers = receivers
        self.proxy = proxy

    def result(self):
        timeout_cond = self._timeout_cond_fn()
        sleep, backoff, counter = .000001, 1000, 1
        sleep_anb_backoff = calc_sleep_and_backoff(0.0001, 1)
        while timeout_cond():
            if res := get_message_from_client_store(self.uuid, self.conn_num):
                if len(res) == 1:
                    raise RemoteCallError(f"Unexpected response: {res[0]}")
                data, flag, serde = res
                if flag == MessageFlag.ERROR:
                    err_name, err_module, err_msg, traceback = data
                    restored_exc = type(err_name, (RemoteCallError,), {"__module__": err_module})
                    if traceback:
                        raise restored_exc(err_msg).with_traceback(traceback)
                    else:
                        raise restored_exc(err_msg)
                return data, flag, serde
            sleep, backoff = sleep_anb_backoff(sleep, backoff)
            time.sleep(sleep)
            counter = (counter + 1) % 1000
            if counter == 0 and self.receivers:
                if missing_receivers := self.receivers - {m["name"] for m in get_available_members(self.conn_num)}:
                    raise TransmissionFailure(
                        f"The anticipated receivers(s) {missing_receivers}"
                        f" unexpectedly disconnected, causing an interruption in the awaiting result of {self.proxy}."
                        f" All receivers: {self.receivers}"
                    )
        else:
            mark_message_as_expired(self.uuid, self.conn_num)
            raise TimeoutError(f"Timeout reached for rpc call with uuid: {self.uuid}.")

    def _timeout_cond_fn(self):
        if self.timeout <= 0:
            return lambda: True
        else:
            timeout = self.send_ts + self.timeout
            return lambda: timeout > time.time()


class Result:

        def __init__(self, results, transmitters):
            self.results = results
            self.transmitters = transmitters

        def __getitem__(self, item):
            return self.results[item]
