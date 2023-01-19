from typing import Dict, Set
from daffi.utils.custom_types import GlobalCallback, K

BYTES_CHUNK = int(4e6)  # 4 Mb
BYTES_LIMIT = int(8e6)  # 8 Mb

RECONNECTION_TIMEOUT = 15  # sec

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
