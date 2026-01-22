# Mamamia Server

The server handles orchestration, storage, and state management.

## Components

- **Orchestrator**: The "brain" that implements the offset sliding logic and lazy lease reaping.
- **Registry**: Manages multiple log instances and their respective backends.
- **Storage**: Append-only log implementation (Default: `InMemoryStorage`).
- **State**: Tracks per-group offsets and per-message processing status (Default: `InMemoryStateStore`).
- **Lease**: Manages time-based locks for concurrency control (Default: `InMemoryLeaseManager`).

## Modularization

Every component implements an interface defined in `mamamia.core.interfaces`. To swap a backend (e.g., to use Redis for leases):
1. Implement `ILeaseManager`.
2. Update the `LogRegistry` in `registry.py` to instantiate your new class.
