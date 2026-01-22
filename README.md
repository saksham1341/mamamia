# Mamamia

Mamamia is a modular, high-performance message delivery system designed for asynchronous processing. It features an append-only log, multi-consumer group support, and just-in-time (JIT) leasing to ensure exactly-once processing within a group.

## Features

- **Append-Only Log**: Reliable message storage with unique indices.
- **Multi-Log Support**: Isolate different message streams within a single server.
- **Consumer Groups**: Multiple consumers can collaborate to process a log, each receiving exclusive access to messages.
- **JIT Leasing**: Consumers lease messages just before processing, preventing hoarding and ensuring responsiveness.
- **Modular Architecture**: Swap Storage, State, and Lease backends (e.g., Memory, Redis, SQL) with ease.
- **REST API**: Simple, extensible interface for producers and consumers.

## Architecture

```text
mamamia/
├── core/               # Shared models and abstract interfaces
├── server/             # Orchestration logic and backend implementations
│   ├── storage/        # Message log storage
│   ├── state/          # Offset and message state tracking
│   ├── lease/          # Time-based lock management
│   └── api/            # FastAPI integration
└── client/             # Python library for producers and consumers
```

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
- **Custom Protocol**: Transition from REST to a custom binary protocol (e.g., over TCP or gRPC) for lower latency and higher throughput.
- **Retry Backoff**: Implement exponential backoff for failed message retries.
- **Management UI**: A web dashboard to monitor log sizes, consumer group offsets, and dead-letter queues.
