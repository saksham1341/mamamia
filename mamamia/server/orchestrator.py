import asyncio
from typing import List, Optional
from mamamia.core.interfaces import IMessageStorage, IStateStore, ILeaseManager
from mamamia.core.models import Message, MessageState


class Orchestrator:
    def __init__(
        self,
        storage: IMessageStorage,
        state_store: IStateStore,
        lease_manager: ILeaseManager,
    ):
        self.storage = storage
        self.state_store = state_store
        self.lease_manager = lease_manager
        self._slide_lock = asyncio.Lock()

    async def poll(self, log_id: str, group_id: str, limit: int = 10) -> List[Message]:
        # 1. Slide the base offset forward first
        await self._slide_offset(log_id, group_id)

        results = []
        current_offset = await self.state_store.get_base_offset(log_id, group_id)
        batch_size = max(limit, 20)

        while len(results) < limit:
            messages = await self.storage.get_batch(log_id, current_offset, batch_size)
            if not messages:
                break

            for msg in messages:
                state = await self.state_store.get_message_state(
                    log_id, group_id, msg.id
                )
                lease = await self.lease_manager.get_lease(log_id, group_id, msg.id)

                if state in (MessageState.PROCESSED, MessageState.DEAD):
                    continue

                if state == MessageState.IN_PROGRESS and not lease:
                    await self.state_store.set_message_state(
                        log_id, group_id, msg.id, MessageState.PENDING
                    )
                    state = MessageState.PENDING

                if state in (MessageState.PENDING, MessageState.FAILED) and not lease:
                    results.append(msg)
                    if len(results) >= limit:
                        break

            current_offset += len(messages)

        return results

    async def acquire_lease(
        self,
        log_id: str,
        group_id: str,
        message_id: int,
        client_id: str,
        duration: float = 30.0,
    ) -> bool:
        state = await self.state_store.get_message_state(log_id, group_id, message_id)

        if state in (MessageState.PROCESSED, MessageState.DEAD):
            return False

        success = await self.lease_manager.acquire(
            log_id, group_id, message_id, client_id, duration
        )
        if success:
            await self.state_store.set_message_state(
                log_id, group_id, message_id, MessageState.IN_PROGRESS
            )
        return success

    async def settle(
        self,
        log_id: str,
        group_id: str,
        message_id: int,
        client_id: str,
        success: bool,
        max_retries: int = 3,
    ):
        lease = await self.lease_manager.get_lease(log_id, group_id, message_id)
        # Allow settlement if lease expired but no one else took it
        if lease and lease.owner_id != client_id:
            raise PermissionError("Client does not own the lease for this message")

        if success:
            new_state = MessageState.PROCESSED
        else:
            retries = await self.state_store.increment_retry_count(
                log_id, group_id, message_id
            )
            if retries >= max_retries:
                new_state = MessageState.DEAD
            else:
                new_state = MessageState.FAILED

        await self.state_store.set_message_state(
            log_id, group_id, message_id, new_state
        )
        await self.lease_manager.release(log_id, group_id, message_id)

        if success or new_state == MessageState.DEAD:
            await self._slide_offset(log_id, group_id)

    async def _slide_offset(self, log_id: str, group_id: str):
        async with self._slide_lock:
            current_offset = await self.state_store.get_base_offset(log_id, group_id)

            while True:
                state = await self.state_store.get_message_state(
                    log_id, group_id, current_offset
                )
                if state in (MessageState.PROCESSED, MessageState.DEAD):
                    current_offset += 1
                else:
                    break

            await self.state_store.set_base_offset(log_id, group_id, current_offset)
