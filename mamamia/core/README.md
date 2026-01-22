# Mamamia Binary Protocol (MAB)

Mamamia uses a custom binary protocol over TCP to achieve high throughput and low latency, significantly outperforming standard HTTP/REST.

## Framing

Every message (Request and Response) is sent as a length-prefixed frame:

| Offset | Field | Size | Type | Description |
| :--- | :--- | :--- | :--- | :--- |
| 0 | **Length** | 4 bytes | Big-endian UInt32 | Total size of the payload (excluding these 4 bytes) |
| 4 | **Version** | 1 byte | UInt8 | Protocol version (Initial: `0x01`) |
| 5 | **Command** | 1 byte | UInt8 | Command or Response ID |
| 6 | **Payload** | N bytes | MessagePack | Serialized data body |

## Commands

### 1. PRODUCE (`0x01`)
Used by producers to append messages to a log.

**Payload:**
```json
{
    "log_id": "string",
    "payload": "any",
    "metadata": "dict|null"
}
```

**Response:**
```json
{
    "message_id": "int"
}
```

### 2. ACQUIRE_NEXT (`0x02`)
Used by consumers to atomically find and lease the next available message.

**Payload:**
```json
{
    "log_id": "string",
    "group_id": "string",
    "client_id": "string",
    "duration": "float"
}
```

**Response:**
```json
{
    "message": {
        "id": "int",
        "log_id": "string",
        "payload": "any",
        "metadata": "dict|null"
    } | null
}
```

### 3. SETTLE (`0x03`)
Used by consumers to mark a message as processed or failed.

**Payload:**
```json
{
    "log_id": "string",
    "group_id": "string",
    "message_id": "int",
    "client_id": "string",
    "success": "bool"
}
```

**Response:**
```json
{
    "status": "settled"
}
```

## Error Handling

If an operation fails, the server returns the same Command ID but the MessagePack payload contains an `error` key:

```json
{
    "error": "Detailed error message"
}
```

## Advantages over HTTP

1. **Persistent Connections**: Avoids TCP/TLS handshake overhead for every request.
2. **Minimal Headers**: No bulky HTTP headers (User-Agent, Content-Type, etc.). MAB header is only 6 bytes.
3. **Binary Serialization**: Uses MessagePack which is faster to parse and smaller than JSON.
4. **Multiplexing Friendly**: Length-prefixed framing allows for easy implementation of asynchronous multiplexing in the future.
