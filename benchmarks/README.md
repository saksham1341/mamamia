# Mamamia Benchmarking Suite

This directory contains tools to evaluate the performance, throughput, latency, and correctness of the Mamamia message delivery system.

## Performance Metrics

The suite measures:
- **Producer Throughput**: Messages per second (msg/s) during ingestion.
- **Consumer Throughput**: Messages per second (msg/s) for the full cycle (Acquire -> Settle).
- **Efficiency**: Throughput per active participant (TPS / Max(Consumers, Producers)). High efficiency indicates the scheduler is not the bottleneck.
- **End-to-End Latency**: Time from message production to successful settlement.
- **Queue Depth**: Maximum number of pending messages in the backlog.
- **In-Flight Leases**: Maximum number of messages concurrently leased but not yet settled.
- **Correctness**: Validates that every message is processed exactly once, with zero duplicates or lost messages.

## Running the Benchmarks

The suite can be configured via a JSON file to run multiple scenarios and generate comparative reports.

### 1. External Server (Recommended for accuracy)
To avoid GIL contention between the server and the benchmark client, start the server in a separate terminal:
```bash
python -m mamamia.server.run --port 9000
```

Then run the benchmark suite:
```bash
python benchmarks/suite.py --addr localhost:9000 --config benchmarks/default_config.json
```

### 2. Internal Server (Convenience)
Run everything in a single process (useful for quick logic verification):
```bash
python benchmarks/suite.py --internal-server
```

## Reports

The suite generates a detailed HTML report (`benchmarks/report.html`) containing:
- **Comparative Table**: Metrics for all configured scenarios side-by-side.
- **Latency Statistics**: Average and P95 latency.
- **Scaling Insights**: Auto-generated observations on optimal concurrency.
- **Correctness Verification**: Confirmation of lease integrity.

## Configuration

The config file (`benchmarks/default_config.json`) defines:
- `output`: CLI and HTML reporting preferences.
- `scenarios`: A list of benchmark runs with specific `producers`, `consumers`, and `msgs` counts.

Example scenario:
```json
{
    "name": "High Concurrency Stress",
    "msgs": 10000,
    "producers": 10,
    "consumers": 50
}
```

## Profiling

To identify bottlenecks, use `py-spy`:
```bash
# Record top functions while benchmarking
py-spy top -- ./venv/bin/python benchmarks/suite.py --internal-server
```
