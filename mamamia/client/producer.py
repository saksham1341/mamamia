from typing import Any, Optional, Union
from mamamia.core.protocol import Command
from mamamia.client.transport import ITransport, TcpTransport


class ProducerClient:
    def __init__(self, transport_or_addr: Union[str, ITransport], log_id: str):
        if isinstance(transport_or_addr, str):
            if ":" in transport_or_addr:
                host, port_str = transport_or_addr.split(":", 1)
                port = int(port_str)
            else:
                host = transport_or_addr
                port = 9000
            self.transport = TcpTransport(host, port)
        else:
            self.transport = transport_or_addr
        self.log_id = log_id

    async def close(self):
        await self.transport.close()

    async def send(self, payload: Any, metadata: Optional[dict] = None) -> int:
        response = await self.transport.request(
            Command.PRODUCE,
            {"log_id": self.log_id, "payload": payload, "metadata": metadata},
        )
        return response["message_id"]
