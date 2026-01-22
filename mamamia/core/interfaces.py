from abc import ABC, abstractmethod
from typing import List, Optional, Any
from .models import Message, MessageState, Lease


class IMessageStorage(ABC):
    @abstractmethod
    async def append(
        self, log_id: str, payload: Any, metadata: Optional[dict] = None
    ) -> int:
        """Appends a message and returns its unique index."""
        pass

    @abstractmethod
    async def get_batch(
        self, log_id: str, start_index: int, limit: int
    ) -> List[Message]:
        """Retrieves a batch of messages starting from start_index."""
        pass


class IStateStore(ABC):
    @abstractmethod
    async def get_base_offset(self, log_id: str, group_id: str) -> int:
        pass

    @abstractmethod
    async def set_base_offset(self, log_id: str, group_id: str, offset: int):
        pass

    @abstractmethod
    async def get_message_state(
        self, log_id: str, group_id: str, message_id: int
    ) -> MessageState:
        pass

    @abstractmethod
    async def set_message_state(
        self, log_id: str, group_id: str, message_id: int, state: MessageState
    ):
        pass

    @abstractmethod
    async def get_retry_count(self, log_id: str, group_id: str, message_id: int) -> int:
        pass

    @abstractmethod
    async def increment_retry_count(
        self, log_id: str, group_id: str, message_id: int
    ) -> int:
        pass


class ILeaseManager(ABC):
    @abstractmethod
    async def acquire(
        self,
        log_id: str,
        group_id: str,
        message_id: int,
        owner_id: str,
        duration: float,
    ) -> bool:
        """Attempts to acquire a lease. Returns True if successful."""
        pass

    @abstractmethod
    async def release(self, log_id: str, group_id: str, message_id: int):
        pass

    @abstractmethod
    async def get_lease(
        self, log_id: str, group_id: str, message_id: int
    ) -> Optional[Lease]:
        pass

    @abstractmethod
    async def reap_expired(self):
        """Removes all expired leases from the manager."""
        pass
