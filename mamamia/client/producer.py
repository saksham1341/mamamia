from typing import Any, Optional, Union
from mamamia.core.protocol import Command
from mamamia.client.transport import ITransport, HttpTransport, TcpTransport


class ProducerClient:
    def __init__(self, transport_or_url: Union[str, ITransport], log_id: str):
        if isinstance(transport_or_url, str):
            if transport_or_url.startswith("http"):
                self.transport = HttpTransport(transport_or_url)
            else:
                host, port = transport_or_url.split(":")
                self.transport = TcpTransport(host, int(port))
        else:
            self.transport = transport_or_url
        self.log_id = log_id

    async def close(self):
        await self.transport.close()

    async def send(self, payload: Any, metadata: Optional[dict] = None) -> int:
        response = await self.transport.request(
            Command.PRODUCE,
            {"log_id": self.log_id, "payload": payload, "metadata": metadata},
        )
        return response["message_id"]
