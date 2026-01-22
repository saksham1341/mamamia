import httpx
import asyncio
import uuid
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


class HttpTransport(ITransport):
    def __init__(self, base_url: str, timeout: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout)

    async def request(self, command: Command, payload: Dict[str, Any]) -> Any:
        if command == Command.PRODUCE:
            log_id = payload.pop("log_id")
            response = await self._client.post(
                f"{self.base_url}/logs/{log_id}/messages", json=payload
            )
            response.raise_for_status()
            return response.json()

        elif command == Command.ACQUIRE_NEXT:
            log_id = payload.pop("log_id")
            group_id = payload.pop("group_id")
            response = await self._client.post(
                f"{self.base_url}/logs/{log_id}/groups/{group_id}/acquire_next",
                json=payload,
            )
            response.raise_for_status()
            return response.json()

        elif command == Command.SETTLE:
            log_id = payload.pop("log_id")
            group_id = payload.pop("group_id")
            message_id = payload.pop("message_id")
            response = await self._client.post(
                f"{self.base_url}/logs/{log_id}/groups/{group_id}/messages/{message_id}/settle",
                json=payload,
            )
            response.raise_for_status()
            return response.json()

        raise ValueError(f"Unknown command for HTTP transport: {command}")

    async def close(self):
        await self._client.aclose()


class TcpTransport(ITransport):
    def __init__(self, host: str, port: int, timeout: float = 60.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

    async def _ensure_connected(self):
        if self._writer is None:
            self._reader, self._writer = await asyncio.open_connection(
                self.host, self.port
            )

    async def request(self, command: Command, payload: Dict[str, Any]) -> Any:
        async with self._lock:
            try:
                await self._ensure_connected()
                writer = self._writer
                reader = self._reader
                if writer is None or reader is None:
                    raise ConnectionError("Failed to connect to server")

                data = pack_message(command, payload)
                writer.write(data)
                await writer.drain()

                version, cmd, body = await read_message(reader)
                if isinstance(body, dict) and "error" in body:
                    raise Exception(body["error"])
                return body
            except (
                asyncio.IncompleteReadError,
                ConnectionResetError,
                BrokenPipeError,
                ConnectionError,
            ):
                # Try to reconnect once
                self._writer = None
                self._reader = None
                await self._ensure_connected()
                writer = self._writer
                reader = self._reader
                if writer is None or reader is None:
                    raise ConnectionError("Failed to reconnect to server")

                data = pack_message(command, payload)
                writer.write(data)
                await writer.drain()
                version, cmd, body = await read_message(reader)
                return body

            except (
                asyncio.IncompleteReadError,
                ConnectionResetError,
                BrokenPipeError,
                ConnectionError,
            ):
                # Try to reconnect once
                self._writer = None
                self._reader = None
                await self._ensure_connected()
                if self._writer is None or self._reader is None:
                    raise ConnectionError("Failed to reconnect to server")

                data = pack_message(command, payload)
                self._writer.write(data)
                await self._writer.drain()
                version, cmd, body = await read_message(self._reader)
                return body

    async def close(self):
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
