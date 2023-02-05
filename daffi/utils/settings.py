import os
from typing import Dict, Set
from daffi.utils.custom_types import GlobalCallback, K

DEBUG = bool(os.getenv("DAFFI_DEBUG"))

BYTES_CHUNK = int(4e6)  # 4 Mb
BYTES_LIMIT = int(8e6)  # 8 Mb

LOCAL_CLASS_CALLBACKS: Set = set()
LOCAL_CALLBACK_MAPPING: Dict[K, GlobalCallback] = dict()

WELL_KNOWN_CALLBACKS: Set[str] = {
    "__transfer_and_call",
    "__async_transfer_and_call",
    "__cancel_scheduled_task",
    "__kill_all",
    "__get_all_period_tasks",
    "<unknown>",
}
