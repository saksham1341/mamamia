# Mamamia

Mamamia is a modular, high-performance message delivery system designed for asynchronous processing. It features an append-only log, multi-consumer group support, and just-in-time (JIT) leasing to ensure exactly-once processing within a group.

## Features

- **Append-Only Log**: Reliable message storage with unique indices.
- **Multi-Log Support**: Isolate different message streams within a single server.
- **Consumer Groups**: Multiple consumers can collaborate to process a log, each receiving exclusive access to messages.
- **JIT Leasing**: Consumers lease messages just before processing, preventing hoarding and ensuring responsiveness.
- **Modular Architecture**: Swap Transports (HTTP, TCP), Storage, State, and Lease backends with ease.
- **High Performance TCP Protocol**: Custom binary protocol utilizing MessagePack for minimal overhead and ultra-low latency.
- **REST API**: Standard extensible interface for easy integration.

## Architecture

```text
mamamia/
├── core/               # Shared models, interfaces, and protocol definitions
├── server/             # Orchestration logic and transport frontends (HTTP, TCP)
│   ├── storage/        # Message log storage
│   ├── state/          # Offset and message state tracking
│   ├── lease/          # Time-based lock management
│   └── api/            # FastAPI & TCP integration
└── client/             # Python library with swapable transports
```

## Performance

Mamamia is designed for high performance. Initial benchmarks comparing the REST (HTTP) transport and the custom Binary (TCP) transport show significant improvements:

| Metric | REST (HTTP) | Binary (TCP) | Improvement |
| :--- | :--- | :--- | :--- |
| **Throughput** | ~160 msg/s | **~2,700 msg/s** | **~17x Faster** |
| **Avg Latency** | ~25ms - 1700ms | **~15ms - 90ms** | **Significant reduction** |

*Note: Benchmarks were performed on a single-worker internal server.*

## Getting Started

> **Note:** Currently, Mamamia only supports single-worker server deployments (e.g., `uvicorn --workers 1`). This is because the only implemented backends are In-Memory, which do not share state across processes.

### 1. Install Dependencies
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .
```

### 2. Start the Server
```bash
uvicorn mamamia.server.api:app --reload
```

## Examples

You can find ready-to-run examples in the `examples/` directory:
- `examples/producer.py`: Sends a batch of messages to the server.
- `examples/consumer.py`: Polls, leases, and processes messages.

## Testing

Run the integration simulation to verify multi-consumer exactly-once processing:
```bash
python tests/simulation.py
```

## Logic: Base Offset Advancement
The `base_offset` of a consumer group only advances when the message at that offset is marked as `PROCESSED` or `DEAD`. This ensures that "gaps" in processing are never skipped, even if later messages are finished out of order.

## Future Work

- **Shared Backends**: Implement Redis or SQL-based backends for `IMessageStorage`, `IStateStore`, and `ILeaseManager` to support multi-worker and multi-instance deployments.
    - **SQLite Backend**: Use SQLite in WAL mode for persistent, multi-worker support on a single node.
    - **Redis Backend**: Use Redis for high-throughput, distributed state and lease management.
- **Custom Protocol**: Transition from REST to a custom binary protocol (e.g., over TCP or gRPC) for lower latency and higher throughput.
- **Retry Backoff**: Implement exponential backoff for failed message retries.
- **Management UI**: A web dashboard to monitor log sizes, consumer group offsets, and dead-letter queues.
