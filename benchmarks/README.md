# Mamamia Benchmarking Suite

This directory contains tools to evaluate the performance, throughput, and latency of the Mamamia message delivery system.

## Performance Metrics

The suite measures:
- **Producer Throughput**: Messages per second (msg/s) during ingestion.
- **Consumer Throughput**: Messages per second (msg/s) for the full cycle (Poll -> Acquire -> Settle).
- **End-to-End Latency**: Time from message production to successful settlement. **Note:** Producer and Consumer run concurrently to provide realistic "in-flight" latency measurements.

Note: The benchmark suite performs send and acquire/settle operations sequentially within each batch loop to ensure reliability and avoid connection pool timeouts.

## Running the Benchmarks

### 1. External Server (Recommended for accuracy)
To avoid GIL contention between the server and the benchmark client, start the server in a separate terminal:
```bash
uvicorn mamamia.server.api:app --host 127.0.0.1 --port 8000 --workers 1
```

Then run the benchmark:
```bash
python benchmarks/suite.py --url http://localhost:8000 --msgs 5000 --producers 2 --consumers 4 --batch 100
```

### 2. Internal Server (Convenience)
Run everything in a single process (useful for quick logic verification):
```bash
python benchmarks/suite.py --internal-server --msgs 1000
```

## Parameters

- `--msgs`: Total number of messages to process (default: 1000).
- `--producers`: Number of concurrent producers (default: 1).
- `--consumers`: Number of concurrent consumers (default: 1).
- `--batch`: Number of messages to poll in each consumer batch (default: 50).
- `--url`: The target server URL.
- `--internal-server`: If set, the script will spin up its own server instance.

## Profiling

To identify bottlenecks, use `py-spy`:
```bash
# Record top functions while benchmarking
py-spy top -- ./venv/bin/python benchmarks/suite.py --internal-server --msgs 5000
```
