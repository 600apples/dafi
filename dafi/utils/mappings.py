import asyncio
from random import choice
from collections import defaultdict
from typing import Dict, DefaultDict, Optional, Tuple, Union, Sequence, Set, List
from dafi.utils.custom_types import GlobalCallback, K


LOCAL_CLASS_CALLBACKS: Set = set()
AWAITED_PROCS: Dict[str, Tuple[str, str]] = dict()
LOCAL_CALLBACK_MAPPING: Dict[K, GlobalCallback] = dict()
NODE_CALLBACK_MAPPING: DefaultDict[K, Dict[K, GlobalCallback]] = defaultdict()
CONTROLLER_CALLBACK_MAPPING: DefaultDict[K, Dict[K, GlobalCallback]] = defaultdict()
SCHEDULER_PERIODICAL_TASKS: Dict[str, asyncio.Task] = dict()
SCHEDULER_AT_TIME_TASKS: Dict[str, List[asyncio.Task]] = dict()

WELL_KNOWN_CALLBACKS: Set[str] = {
    "__transfer_and_call",
    "__async_transfer_and_call",
    "__cancel_scheduled_task",
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
