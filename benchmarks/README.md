# Mamamia Benchmarking Suite

This directory contains tools to evaluate the performance, throughput, and latency of the Mamamia message delivery system.

## Performance Metrics

The suite measures:
- **Producer Throughput**: Messages per second (msg/s) during ingestion.
- **Consumer Throughput**: Messages per second (msg/s) for the full cycle (Poll -> Acquire -> Settle).
- **End-to-End Latency**: Time from message production to successful settlement. **Note:** Producer and Consumer run concurrently to provide realistic "in-flight" latency measurements.

Note: The benchmark suite performs send and acquire/settle operations sequentially within each batch loop to ensure reliability and avoid connection pool timeouts.

## Running the Benchmarks

The suite can be configured via a JSON file to run multiple scenarios and generate comparative reports.

### 1. External Server (Recommended for accuracy)
To avoid GIL contention between the server and the benchmark client, start the server in a separate terminal:
```bash
uvicorn mamamia.server.api:app --host 127.0.0.1 --port 8000 --workers 1
```

Then run the benchmark suite:
```bash
python benchmarks/suite.py --url http://localhost:8000 --config benchmarks/default_config.json
```

### 2. Internal Server (Convenience)
Run everything in a single process:
```bash
python benchmarks/suite.py --internal-server
```

## Configuration

The config file (`benchmarks/default_config.json`) defines:
- `output`: CLI and HTML reporting preferences.
- `scenarios`: A list of benchmark runs with specific `producers`, `consumers`, and `msgs` counts.

Example scenario:
```json
{
    "name": "High Concurrency Stress",
    "msgs": 5000,
    "producers": 5,
    "consumers": 10,
    "batch": 100
}
```


## Profiling

To identify bottlenecks, use `py-spy`:
```bash
# Record top functions while benchmarking
py-spy top -- ./venv/bin/python benchmarks/suite.py --internal-server
```
