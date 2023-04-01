import math
from datetime import datetime
from cachetools import TTLCache
from typing import Optional, Any

from daffi.utils.misc import call_after
from daffi.settings import STORE_EXPIRATION_TTL


class TttStore(TTLCache):
    def __init__(self):
        super().__init__(maxsize=math.inf, ttl=STORE_EXPIRATION_TTL, timer=datetime.now)

    async def set(self, key: Any, value: Any, ttl: Optional[int] = None, _delay: Optional[int] = 1):
        """
        Add item to store.
        Args:
            ttl: time in seconds
                If ttl argument provided item will be deleted after specified ttl
            _delay: Optional delay. If ttl provided then total ttl = ttl + _delay
        """
        self[key] = value
        if ttl:
            await call_after(ttl + _delay, self.pop, key, None)
