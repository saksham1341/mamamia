# Mamamia

Mamamia is a high-performance, modular message delivery system designed for asynchronous processing. It features an append-only log, multi-consumer group support, and strictly JIT (Just-In-Time) leasing via an efficient binary protocol over TCP.

## Features

- **Append-Only Log**: Reliable message storage with unique indices.
- **Multi-Log Support**: Isolate different message streams within a single server.
- **Consumer Groups**: Multiple consumers can collaborate to process a log, each receiving exclusive access to messages.
- **Atomic JIT Leasing**: Consumers lease messages just before processing using the `acquire_next` atomic operation, preventing collisions and ensuring responsiveness.
- **Binary TCP Protocol**: Custom ultra-low latency protocol utilizing MessagePack and length-prefixed framing.
- **Modular Architecture**: Swap Storage, State, and Lease backends with ease.

## Performance

Mamamia is optimized for speed. Benchmarks on a single-worker server show:
- **Throughput**: ~2,500 messages per second.
- **Latency**: ~15ms - 90ms (E2E).

## Architecture

```text
mamamia/
├── core/               # Shared models, interfaces, and binary protocol
├── server/             # Orchestration logic and TCP frontend
│   ├── storage/        # Message log storage
│   ├── state/          # Offset and message state tracking
│   ├── lease/          # Time-based lock management
│   └── run.py          # Server entry point
└── client/             # Python library with binary transport
```

## Getting Started

### 1. Install Dependencies
```bash
python -m venv venv
source venv/bin/activate
pip install -e .
```

### 2. Start the Server
```bash
python -m mamamia.server.run --port 9000
```

### 3. Usage Example

**Producer:**
```python
from mamamia.client.producer import ProducerClient
import asyncio

async def main():
    # Connect to local server via TCP
    producer = ProducerClient("localhost:9000", log_id="orders")
    await producer.send({"order_id": 123, "amount": 99.99})
    await producer.close()

asyncio.run(main())
```

**Consumer:**
```python
from mamamia.client.consumer import ConsumerClient
import asyncio

async def main():
    consumer = ConsumerClient(
        "localhost:9000", 
        log_id="orders", 
        group_id="processing-service"
    )
    
    while True:
        # Atomically find and lease the next available message
        msg = await consumer.acquire_next(duration=30.0)
        if msg:
            print(f"Processing: {msg['payload']}")
            await consumer.settle(msg["id"], success=True)
        else:
            await asyncio.sleep(1)

asyncio.run(main())
```

## Examples

You can find ready-to-run examples in the `examples/` directory:
- `examples/producer.py`: Sends a batch of messages.
- `examples/consumer.py`: Polls and processes messages.

## Testing

Run the integration simulation:
```bash
python tests/simulation.py
```

## Benchmarking

Run the performance suite:
```bash
python benchmarks/suite.py --internal-server
```

## Future Work

- **Shared Backends**: Implement Redis or SQL-based backends for `IMessageStorage`, `IStateStore`, and `ILeaseManager` to support multi-worker and multi-instance deployments.
- **Multiplexing**: Support multiple concurrent requests over a single TCP connection.
- **Retry Backoff**: Implement exponential backoff for failed message retries.
- **Management UI**: A dashboard to monitor logs and consumer groups.
