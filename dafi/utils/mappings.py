import asyncio
from collections import defaultdict
from typing import Dict, DefaultDict, Optional, Tuple, Union, Sequence, Set
from dafi.utils.custom_types import GlobalCallback, K


LOCAL_CLASS_CALLBACKS: Set = set()
AWAITED_PROCS: Dict[str, Tuple[str, str]] = dict()
LOCAL_CALLBACK_MAPPING: Dict[K, GlobalCallback] = dict()
NODE_CALLBACK_MAPPING: DefaultDict[K, Dict[K, GlobalCallback]] = defaultdict()
CONTROLLER_CALLBACK_MAPPING: DefaultDict[K, Dict[K, GlobalCallback]] = defaultdict()
SCHEDULER_PERIODICAL_TASKS: Dict[str, asyncio.Task] = dict()

WELL_KNOWN_CALLBACKS: Set[str] = {"__transfer_and_call", "__async_transfer_and_call"}


def search_remote_callback_in_mapping(
    mapping: DefaultDict[K, Dict[K, GlobalCallback]],
    func_name: str,
    exclude: Optional[Union[str, Sequence]] = None,
) -> Optional[Tuple[str, "RemoteCallback"]]:

    if isinstance(exclude, str):
        exclude = [exclude]
    exclude = exclude or []

    for proc, func_mapping in mapping.items():
        if proc not in exclude:
            remote_callback = func_mapping.get(func_name)
            if remote_callback:
                return proc, remote_callback
