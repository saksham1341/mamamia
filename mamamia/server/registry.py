import asyncio
from typing import Dict
from .orchestrator import Orchestrator
from .storage.in_memory import InMemoryStorage
from .state.in_memory import InMemoryStateStore
from .lease.in_memory import InMemoryLeaseManager


class LogRegistry:
    def __init__(self):
        self._orchestrators: Dict[str, Orchestrator] = {}
        self._shared_storage = InMemoryStorage()
        self._shared_state = InMemoryStateStore()
        self._shared_lease = InMemoryLeaseManager()
        self._reaper_task = None

    def start_reaper(self, interval: float = 60.0):
        if self._reaper_task is None:
            self._reaper_task = asyncio.create_task(self._reap_loop(interval))

    async def _reap_loop(self, interval: float):
        while True:
            await asyncio.sleep(interval)
            await self._shared_lease.reap_expired()

    def get_orchestrator(self, log_id: str) -> Orchestrator:
        if log_id not in self._orchestrators:
            # In a more complex system, we could initialize different
            # storage backends based on log_id config.
            self._orchestrators[log_id] = Orchestrator(
                self._shared_storage, self._shared_state, self._shared_lease
            )
        return self._orchestrators[log_id]

    def get_storage(self):
        return self._shared_storage
