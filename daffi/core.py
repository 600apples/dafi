import ctypes
import dfcore
import json
from typing import Union, Tuple

class MessageFlag:
    HANDSHAKE = 0
    REQUEST = 1
    RESPONSE = 2
    ERROR = 3
    EVENTS = 4


def send_message_from_client(
        data: Union[str, bytes], flag: MessageFlag, serde: int, receiver: str,
        func_name: str, return_result: bool, conn_num: int, is_bytes: bool = True, uuid: int = 0
) -> Tuple[int, int, str]:
    if is_bytes:
        data = bytes(data)
        data = ctypes.create_string_buffer(data, len(data))
    else:
        data = str(data)
        data = ctypes.create_string_buffer(data.encode(), len(data))
    return dfcore.sendMessageFromClient(
        data, uuid, flag, serde, is_bytes, receiver, func_name, return_result, conn_num
    )


def send_handshake_from_client(password: str, methods: str, conn_num: int) -> Tuple[int, int, str]:
    return dfcore.sendHandshakeFromClient(password, methods, conn_num)


def get_available_members(conn_num: int) -> list:
    members = dfcore.getAvailableMembers(conn_num)
    return json.loads(members)["members"] if members else []


def send_message_from_service(
        data: Union[str, bytes], flag: MessageFlag, serde: int, receiver: str,
        func_name: str, return_result: bool, conn_num: int, is_bytes: bool = True, uuid: int = 0
) -> Tuple[int, int, str]:
    if is_bytes:
        data = bytes(data)
        data = ctypes.create_string_buffer(data, len(data))
    else:
        data = str(data)
        data = ctypes.create_string_buffer(data.encode(), len(data))
    return dfcore.sendMessageFromServer(
        data, uuid, flag, serde, is_bytes, receiver, func_name, return_result, conn_num
    )


def get_message_from_client_store(uuid: int, conn_num: int) -> Tuple:
    return dfcore.getMessageFromClientStore(uuid, conn_num)


def mark_message_as_expired(uuid: int, conn_num: int) -> None:
    dfcore.setTimeoutError(uuid, conn_num)


def set_service_methods(methods: str, conn_num: int) -> None:
    dfcore.setServiceMethods(methods, conn_num)
