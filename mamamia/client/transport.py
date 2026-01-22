import asyncio
import struct
import msgpack
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from mamamia.core.protocol import Command, pack_message, read_message


class ITransport(ABC):
    @abstractmethod
    async def request(self, command: Command, payload: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    async def close(self):
        pass


class TcpTransport(ITransport):
    def __init__(self, host: str, port: int, timeout: float = 60.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

    async def _ensure_connected(self):
        if self._writer is None or self._reader is None:
            self._reader, self._writer = await asyncio.open_connection(
                self.host, self.port
            )

    async def _send_and_receive(self, command: Command, payload: Dict[str, Any]) -> Any:
        await self._ensure_connected()

        # We know they are not None because of _ensure_connected()
        writer: asyncio.StreamWriter = self._writer  # type: ignore
        reader: asyncio.StreamReader = self._reader  # type: ignore

        data = pack_message(command, payload)
        writer.write(data)
        await writer.drain()

        version, cmd, body = await read_message(reader)
        if isinstance(body, dict) and "error" in body:
            raise Exception(body["error"])
        return body

    async def request(self, command: Command, payload: Dict[str, Any]) -> Any:
        async with self._lock:
            try:
                return await self._send_and_receive(command, payload)
            except (
                asyncio.IncompleteReadError,
                ConnectionResetError,
                BrokenPipeError,
                ConnectionError,
                OSError,
            ):
                # Try to reconnect once
                self._writer = None
                self._reader = None
                return await self._send_and_receive(command, payload)

    async def close(self):
        async with self._lock:
            if self._writer:
                self._writer.close()
                try:
                    await self._writer.wait_closed()
                except:
                    pass
                self._writer = None
                self._reader = None
