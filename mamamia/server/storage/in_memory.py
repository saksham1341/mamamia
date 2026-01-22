import asyncio
from typing import List, Optional, Any, Dict
from mamamia.core.interfaces import IMessageStorage
from mamamia.core.models import Message


class InMemoryStorage(IMessageStorage):
    def __init__(self):
        # log_id -> List[Message]
        self._logs: Dict[str, List[Message]] = {}
        # log_id -> Lock
        self._locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    def _get_lock(self, log_id: str) -> asyncio.Lock:
        if log_id not in self._locks:
            self._locks[log_id] = asyncio.Lock()
        return self._locks[log_id]

    async def append(
        self, log_id: str, payload: Any, metadata: Optional[dict] = None
    ) -> int:
        async with self._global_lock:
            lock = self._get_lock(log_id)

        async with lock:
            if log_id not in self._logs:
                self._logs[log_id] = []

            msg_id = len(self._logs[log_id])
            message = Message(
                id=msg_id, log_id=log_id, payload=payload, metadata=metadata
            )
            self._logs[log_id].append(message)
            return msg_id

    async def get_batch(
        self, log_id: str, start_index: int, limit: int
    ) -> List[Message]:
        async with self._global_lock:
            lock = self._get_lock(log_id)

        async with lock:
            log = self._logs.get(log_id, [])
            if start_index >= len(log):
                return []
            return log[start_index : start_index + limit]
