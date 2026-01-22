import asyncio
import time
import statistics
import argparse
import uvicorn
from mamamia.server.api import app
from mamamia.client.producer import ProducerClient
from mamamia.client.consumer import ConsumerClient


async def run_producer_bench(url, log_id, count, batch_size):
    producer = ProducerClient(url, log_id)
    start_time = time.perf_counter()

    for _ in range(count):
        await producer.send(payload={"data": "bench"}, metadata={"ts": time.time()})

    duration = time.perf_counter() - start_time
    await producer.close()
    return count, duration


async def run_consumer_bench(url, log_id, group_id, count, batch_size):
    consumer = ConsumerClient(url, log_id, group_id)
    start_time = time.perf_counter()
    processed = 0
    latencies = []

    while processed < count:
        messages = await consumer.poll(limit=batch_size)
        if not messages:
            await asyncio.sleep(0.1)
            continue

        for msg in messages:
            success = await consumer.acquire(msg["id"])
            if success:
                await consumer.settle(msg["id"], success=True)
                processed += 1
                if "ts" in (msg.get("metadata") or {}):
                    latencies.append(time.time() - msg["metadata"]["ts"])

            if processed >= count:
                break

    duration = time.perf_counter() - start_time
    await consumer.close()
    return processed, duration, latencies


async def main():
    parser = argparse.ArgumentParser(description="Mamamia Benchmarking Suite")
    parser.add_argument("--url", default="http://localhost:8002", help="Server URL")
    parser.add_argument("--msgs", type=int, default=1000, help="Number of messages")
    parser.add_argument(
        "--batch", type=int, default=50, help="Batch size for concurrent requests"
    )
    parser.add_argument(
        "--internal-server",
        action="store_true",
        help="Start an internal server for the test",
    )

    args = parser.parse_args()

    server_task = None
    server = None
    if args.internal_server:
        config = uvicorn.Config(app, host="127.0.0.1", port=8002, log_level="error")
        server = uvicorn.Server(config)
        server_task = asyncio.create_task(server.serve())
        await asyncio.sleep(1)  # Wait for server startup
        print("Internal server started.")

    log_id = f"bench-log-{int(time.time())}"
    group_id = "bench-group"

    print(f"\n--- Starting Producer Benchmark ({args.msgs} messages) ---")
    p_count, p_duration = await run_producer_bench(
        args.url, log_id, args.msgs, args.batch
    )
    print(f"Result: {p_count} messages produced in {p_duration:.2f}s")
    print(f"Throughput: {p_count / p_duration:.2f} msg/s")

    print(f"\n--- Starting Consumer Benchmark ({args.msgs} messages) ---")
    c_count, c_duration, latencies = await run_consumer_bench(
        args.url, log_id, group_id, args.msgs, args.batch
    )
    print(f"Result: {c_count} messages consumed in {c_duration:.2f}s")
    print(f"Throughput: {c_count / c_duration:.2f} msg/s")

    if latencies:
        print(f"Avg E2E Latency: {statistics.mean(latencies) * 1000:.2f}ms")
        print(f"Min Latency: {min(latencies) * 1000:.2f}ms")
        print(f"Max Latency: {max(latencies) * 1000:.2f}ms")

    if server_task and server:
        server.should_exit = True
        await server_task
        print("\nInternal server stopped.")


if __name__ == "__main__":
    asyncio.run(main())
