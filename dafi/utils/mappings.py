from collections import defaultdict
from typing import Dict, DefaultDict, Optional, Tuple, Union, Sequence, Set
from dafi.utils.custom_types import GlobalCallback, K


LOCAL_CLASS_CALLBACKS: Set = set()
AWAITED_PROCS: Dict[str, Tuple[str, str]] = dict()
LOCAL_CALLBACK_MAPPING: Dict[K, GlobalCallback] = dict()
NODE_CALLBACK_MAPPING: DefaultDict[K, Dict[K, GlobalCallback]] = defaultdict()
CONTROLLER_CALLBACK_MAPPING: DefaultDict[K, Dict[K, GlobalCallback]] = defaultdict()


def search_cb_info_in_mapping(
    mapping: DefaultDict[K, Dict[K, GlobalCallback]],
    func_name: str,
    exclude: Optional[Union[str, Sequence]] = None,
) -> Optional[Tuple[str, "CallbackInfo"]]:

    if isinstance(exclude, str):
        exclude = [exclude]
    exclude = exclude or []

    for proc, func_mapping in mapping.items():
        if proc not in exclude:
            cb_info = func_mapping.get(func_name)
            if cb_info:
                return proc, cb_info
