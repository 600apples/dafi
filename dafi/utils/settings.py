import asyncio
from random import choice
from typing import Dict, DefaultDict, Optional, Tuple, Union, Sequence, Set, List, Any
from dafi.utils.custom_types import GlobalCallback, K

BYTES_CHUNK = 4096
BYTES_LIMIT = 1e7  # 10 Mb

LOCAL_CLASS_CALLBACKS: Set = set()
AWAITED_PROCS: Dict[str, Tuple[str, str]] = dict()
AWAITED_BROADCAST_PROCS: Dict[str, Tuple[str, Dict[str, Any]]] = dict()
LOCAL_CALLBACK_MAPPING: Dict[K, GlobalCallback] = dict()
NODE_CALLBACK_MAPPING: Dict[K, Dict[K, GlobalCallback]] = dict()
CONTROLLER_CALLBACK_MAPPING: Dict[K, Dict[K, GlobalCallback]] = dict()
SCHEDULER_PERIODICAL_TASKS: Dict[str, asyncio.Task] = dict()
SCHEDULER_AT_TIME_TASKS: Dict[str, List[asyncio.Task]] = dict()

BROADCAST_RESULT_EMPTY = object()

WELL_KNOWN_CALLBACKS: Set[str] = {
    "__transfer_and_call",
    "__async_transfer_and_call",
    "__cancel_scheduled_task",
    "__kill_all",
}


def search_remote_callback_in_mapping(
    mapping: DefaultDict[K, Dict[K, GlobalCallback]],
    func_name: str,
    exclude: Optional[Union[str, Sequence]] = None,
    take_all: Optional[bool] = False,
) -> Optional[Union[Tuple[str, "RemoteCallback"], List[Tuple[str, "RemoteCallback"]]]]:

    if isinstance(exclude, str):
        exclude = [exclude]
    exclude = exclude or []
    found = []

    for proc, func_mapping in mapping.items():
        if proc not in exclude:
            remote_callback = func_mapping.get(func_name)
            if remote_callback:
                found.append((proc, remote_callback))
    try:
        if take_all:
            return found
        return choice(found)
    except IndexError:
        ...
