import asyncio
import time
from typing import Dict, Tuple, Optional
from mamamia.core.interfaces import ILeaseManager
from mamamia.core.models import Lease


class InMemoryLeaseManager(ILeaseManager):
    def __init__(self):
        # (log_id, group_id, message_id) -> Lease
        self._leases: Dict[Tuple[str, str, int], Lease] = {}
        # (log_id, group_id) -> Lock
        self._locks: Dict[Tuple[str, str], asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    def _get_lock(self, log_id: str, group_id: str) -> asyncio.Lock:
        key = (log_id, group_id)
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def acquire(
        self,
        log_id: str,
        group_id: str,
        message_id: int,
        owner_id: str,
        duration: float,
    ) -> bool:
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)

        async with lock:
            key = (log_id, group_id, message_id)
            now = time.time()

            existing = self._leases.get(key)
            if existing and existing.expiry > now:
                return False

            self._leases[key] = Lease(owner_id=owner_id, expiry=now + duration)
            return True

    async def release(self, log_id: str, group_id: str, message_id: int):
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)
        async with lock:
            self._leases.pop((log_id, group_id, message_id), None)

    async def get_lease(
        self, log_id: str, group_id: str, message_id: int
    ) -> Optional[Lease]:
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)
        async with lock:
            key = (log_id, group_id, message_id)
            lease = self._leases.get(key)
            if lease and lease.expiry < time.time():
                # Lazy cleanup
                del self._leases[key]
                return None
            return lease

    async def reap_expired(self):
        async with self._global_lock:
            now = time.time()
            expired_keys = [
                key for key, lease in self._leases.items() if lease.expiry < now
            ]
            for key in expired_keys:
                del self._leases[key]
