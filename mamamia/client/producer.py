import httpx
from typing import Any, Optional


class ProducerClient:
    def __init__(self, base_url: str, log_id: str):
        self.base_url = base_url.rstrip("/")
        self.log_id = log_id
        self._client = httpx.AsyncClient()

    async def close(self):
        await self._client.aclose()

    async def send(self, payload: Any, metadata: Optional[dict] = None) -> int:
        response = await self._client.post(
            f"{self.base_url}/logs/{self.log_id}/messages",
            json={"payload": payload, "metadata": metadata},
        )
        response.raise_for_status()
        return response.json()["message_id"]
