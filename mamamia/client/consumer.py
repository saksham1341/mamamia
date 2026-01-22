import httpx
import uuid
import asyncio
from typing import List, Any, Optional, Dict
from mamamia.core.models import Message


class ConsumerClient:
    def __init__(
        self,
        base_url: str,
        log_id: str,
        group_id: str,
        client_id: Optional[str] = None,
        timeout: float = 60.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.log_id = log_id
        self.group_id = group_id
        self.client_id = client_id or str(uuid.uuid4())
        self._client = httpx.AsyncClient(timeout=timeout)

    async def close(self):
        await self._client.aclose()

    async def acquire_next(self, duration: float = 30.0) -> Optional[Dict[str, Any]]:
        response = await self._client.post(
            f"{self.base_url}/logs/{self.log_id}/groups/{self.group_id}/acquire_next",
            json={"client_id": self.client_id, "duration": duration},
        )
        response.raise_for_status()
        return response.json()["message"]

    async def settle(self, message_id: int, success: bool):
        response = await self._client.post(
            f"{self.base_url}/logs/{self.log_id}/groups/{self.group_id}/messages/{message_id}/settle",
            json={"client_id": self.client_id, "success": success},
        )
        response.raise_for_status()
