import msgpack
import struct
import asyncio
from enum import IntEnum
from typing import Any, Dict, Optional, Tuple


class Command(IntEnum):
    PRODUCE = 1
    ACQUIRE_NEXT = 2
    SETTLE = 3


MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10MB limit


def pack_message(command: int, body: Any) -> bytes:
    """Pack a message into [length(4)][version(1)][command(1)][msgpack_body]."""
    packed_body = msgpack.packb(body)
    if not isinstance(packed_body, bytes):
        raise TypeError("msgpack.packb did not return bytes")

    version = 1
    # header: version(1) + command(1)
    header = struct.pack("!BB", version, command)
    full_body = header + packed_body
    length = len(full_body)
    return struct.pack("!I", length) + full_body


async def read_message(reader: asyncio.StreamReader) -> Tuple[int, int, Any]:
    """Read a message from an asyncio reader."""
    length_bytes = await reader.readexactly(4)
    length = struct.unpack("!I", length_bytes)[0]

    if length > MAX_MESSAGE_SIZE:
        raise ValueError(f"Message size {length} exceeds limit {MAX_MESSAGE_SIZE}")

    data = await reader.readexactly(length)
    version, command = struct.unpack("!BB", data[:2])
    body = msgpack.unpackb(data[2:])
    return version, command, body
