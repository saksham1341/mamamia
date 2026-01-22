import uuid
from typing import Any, Optional, Dict, Union
from mamamia.core.protocol import Command
from mamamia.client.transport import ITransport, HttpTransport, TcpTransport


class ConsumerClient:
    def __init__(
        self,
        transport_or_url: Union[str, ITransport],
        log_id: str,
        group_id: str,
        client_id: Optional[str] = None,
    ):
        if isinstance(transport_or_url, str):
            if transport_or_url.startswith("http"):
                self.transport = HttpTransport(transport_or_url)
            else:
                host, port = transport_or_url.split(":")
                self.transport = TcpTransport(host, int(port))
        else:
            self.transport = transport_or_url

        self.log_id = log_id
        self.group_id = group_id
        self.client_id = client_id or str(uuid.uuid4())

    async def close(self):
        await self.transport.close()

    async def acquire_next(self, duration: float = 30.0) -> Optional[Dict[str, Any]]:
        response = await self.transport.request(
            Command.ACQUIRE_NEXT,
            {
                "log_id": self.log_id,
                "group_id": self.group_id,
                "client_id": self.client_id,
                "duration": duration,
            },
        )
        return response["message"]

    async def settle(self, message_id: int, success: bool):
        await self.transport.request(
            Command.SETTLE,
            {
                "log_id": self.log_id,
                "group_id": self.group_id,
                "message_id": message_id,
                "client_id": self.client_id,
                "success": success,
            },
        )
