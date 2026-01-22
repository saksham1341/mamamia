import asyncio
import logging
from typing import Optional
from mamamia.core.protocol import Command, read_message, pack_message
from mamamia.server.registry import LogRegistry

logger = logging.getLogger(__name__)


class TcpFrontend:
    def __init__(self, registry: LogRegistry, host: str = "0.0.0.0", port: int = 9000):
        self.registry = registry
        self.host = host
        self.port = port
        self._server: Optional[asyncio.Server] = None

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        addr = writer.get_extra_info("peername")
        logger.debug(f"New connection from {addr}")

        try:
            while True:
                try:
                    version, command, body = await read_message(reader)
                except asyncio.IncompleteReadError:
                    break

                response_body = await self.process_command(command, body)
                writer.write(pack_message(command, response_body))
                await writer.drain()
        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def process_command(self, command: int, body: dict) -> dict:
        try:
            if command == Command.PRODUCE:
                log_id = body["log_id"]
                storage = self.registry.get_storage()
                msg_id = await storage.append(
                    log_id, body["payload"], body.get("metadata")
                )
                return {"message_id": msg_id}

            elif command == Command.ACQUIRE_NEXT:
                log_id = body["log_id"]
                group_id = body["group_id"]
                orch = self.registry.get_orchestrator(log_id)
                message = await orch.acquire_next(
                    log_id, group_id, body["client_id"], body.get("duration", 30.0)
                )
                if not message:
                    return {"message": None}
                # Pydantic model to dict
                return {
                    "message": message.model_dump()
                    if hasattr(message, "model_dump")
                    else message.dict()
                }

            elif command == Command.SETTLE:
                log_id = body["log_id"]
                group_id = body["group_id"]
                orch = self.registry.get_orchestrator(log_id)
                await orch.settle(
                    log_id,
                    group_id,
                    body["message_id"],
                    body["client_id"],
                    body["success"],
                )
                return {"status": "settled"}

            return {"error": f"Unknown command: {command}"}
        except Exception as e:
            logger.exception("Error processing command")
            return {"error": str(e)}

    async def start(self):
        self._server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        addr = self._server.sockets[0].getsockname()
        logger.info(f"TCP Frontend serving on {addr}")
        async with self._server:
            await self._server.serve_forever()

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
