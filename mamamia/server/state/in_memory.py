import asyncio
from typing import Dict, Tuple, List
from mamamia.core.interfaces import IStateStore
from mamamia.core.models import MessageState


class InMemoryStateStore(IStateStore):
    def __init__(self):
        # (log_id, group_id) -> base_offset
        self._offsets: Dict[Tuple[str, str], int] = {}
        # (log_id, group_id, message_id) -> MessageState
        self._states: Dict[Tuple[str, str, int], MessageState] = {}
        # (log_id, group_id, message_id) -> retry_count
        self._retries: Dict[Tuple[str, str, int], int] = {}
        # (log_id, group_id) -> Lock
        self._locks: Dict[Tuple[str, str], asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    def _get_lock(self, log_id: str, group_id: str) -> asyncio.Lock:
        key = (log_id, group_id)
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def get_base_offset(self, log_id: str, group_id: str) -> int:
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)
        async with lock:
            return self._offsets.get((log_id, group_id), 0)

    async def set_base_offset(self, log_id: str, group_id: str, offset: int):
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)
        async with lock:
            self._offsets[(log_id, group_id)] = offset

    async def get_message_state(
        self, log_id: str, group_id: str, message_id: int
    ) -> MessageState:
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)
        async with lock:
            return self._states.get(
                (log_id, group_id, message_id), MessageState.PENDING
            )

    async def get_message_states(
        self, log_id: str, group_id: str, message_ids: List[int]
    ) -> Dict[int, MessageState]:
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)
        async with lock:
            return {
                mid: self._states.get((log_id, group_id, mid), MessageState.PENDING)
                for mid in message_ids
            }

    async def set_message_state(
        self, log_id: str, group_id: str, message_id: int, state: MessageState
    ):
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)
        async with lock:
            self._states[(log_id, group_id, message_id)] = state

    async def get_retry_count(self, log_id: str, group_id: str, message_id: int) -> int:
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)
        async with lock:
            return self._retries.get((log_id, group_id, message_id), 0)

    async def increment_retry_count(
        self, log_id: str, group_id: str, message_id: int
    ) -> int:
        async with self._global_lock:
            lock = self._get_lock(log_id, group_id)
        async with lock:
            key = (log_id, group_id, message_id)
            count = self._retries.get(key, 0) + 1
            self._retries[key] = count
            return count
