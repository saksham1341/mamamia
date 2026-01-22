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

    async def poll(self, limit: int = 10) -> List[Dict[str, Any]]:
        response = await self._client.get(
            f"{self.base_url}/logs/{self.log_id}/groups/{self.group_id}/poll",
            params={"limit": limit},
        )
        response.raise_for_status()
        return response.json()["messages"]

    async def acquire(self, message_id: int, duration: float = 30.0) -> bool:
        try:
            response = await self._client.post(
                f"{self.base_url}/logs/{self.log_id}/groups/{self.group_id}/messages/{message_id}/acquire",
                json={"client_id": self.client_id, "duration": duration},
            )
            return response.status_code == 200
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                return False
            raise

    async def settle(self, message_id: int, success: bool):
        response = await self._client.post(
            f"{self.base_url}/logs/{self.log_id}/groups/{self.group_id}/messages/{message_id}/settle",
            json={"client_id": self.client_id, "success": success},
        )
        response.raise_for_status()
