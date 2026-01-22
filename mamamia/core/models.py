from enum import Enum
from pydantic import BaseModel
from typing import Optional, Any


class MessageState(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    PROCESSED = "processed"
    FAILED = "failed"
    DEAD = "dead"


class Message(BaseModel):
    id: int
    log_id: str
    payload: Any
    metadata: Optional[dict] = None


class Lease(BaseModel):
    owner_id: str
    expiry: float  # Unix timestamp
