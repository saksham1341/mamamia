import asyncio
import time
import statistics
import argparse
import uvicorn
from mamamia.server.api import app
from mamamia.client.producer import ProducerClient
from mamamia.client.consumer import ConsumerClient


async def run_producer_bench(url, log_id, count, producer_id):
    producer = ProducerClient(url, log_id)
    start_time = time.perf_counter()

    for i in range(count):
        await producer.send(
            payload={"data": f"bench-{producer_id}-{i}"}, metadata={"ts": time.time()}
        )

    duration = time.perf_counter() - start_time
    await producer.close()
    return count, duration


async def run_consumer_bench(
    url, log_id, group_id, target_count, batch_size, shared_state
):
    consumer = ConsumerClient(url, log_id, group_id)
    start_time = time.perf_counter()
    processed_locally = 0
    latencies = []

    while shared_state["processed"] < target_count:
        # Stop if we've been running too long without progress (safety)
        messages = await consumer.poll(limit=batch_size)
        if not messages:
            if shared_state["processed"] >= target_count:
                break
            await asyncio.sleep(0.1)
            continue

        for msg in messages:
            if shared_state["processed"] >= target_count:
                break

            success = await consumer.acquire(msg["id"])
            if success:
                await consumer.settle(msg["id"], success=True)
                shared_state["processed"] += 1
                processed_locally += 1
                if "ts" in (msg.get("metadata") or {}):
                    latencies.append(time.time() - msg["metadata"]["ts"])

            if shared_state["processed"] >= target_count:
                break

    duration = time.perf_counter() - start_time
    await consumer.close()
    return processed_locally, duration, latencies


async def main():
    parser = argparse.ArgumentParser(description="Mamamia Benchmarking Suite")
    parser.add_argument("--url", default="http://localhost:8002", help="Server URL")
    parser.add_argument(
        "--msgs", type=int, default=1000, help="Total number of messages"
    )
    parser.add_argument("--producers", type=int, default=1, help="Number of producers")
    parser.add_argument("--consumers", type=int, default=1, help="Number of consumers")
    parser.add_argument(
        "--batch", type=int, default=50, help="Batch size for consumer polling"
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

    print(f"\n--- Starting Concurrent Benchmark ---")
    print(f"Total Messages: {args.msgs}")
    print(f"Producers:      {args.producers}")
    print(f"Consumers:      {args.consumers}")

    start_bench = time.perf_counter()

    # Distribute messages among producers
    msgs_per_producer = args.msgs // args.producers
    producer_tasks = []
    for i in range(args.producers):
        count = msgs_per_producer + (1 if i < args.msgs % args.producers else 0)
        producer_tasks.append(run_producer_bench(args.url, log_id, count, i))

    # Shared state for consumers to know when to stop
    shared_state = {"processed": 0}
    consumer_tasks = []
    for i in range(args.consumers):
        consumer_tasks.append(
            run_consumer_bench(
                args.url, log_id, group_id, args.msgs, args.batch, shared_state
            )
        )

    # Run producers and consumers in parallel
    results = await asyncio.gather(
        asyncio.gather(*producer_tasks), asyncio.gather(*consumer_tasks)
    )

    total_duration = time.perf_counter() - start_bench
    p_results, c_results = results

    total_p_count = sum(r[0] for r in p_results)
    avg_p_duration = sum(r[1] for r in p_results) / len(p_results)

    total_c_count = sum(r[0] for r in c_results)
    avg_c_duration = sum(r[1] for r in c_results) / len(c_results)

    all_latencies = []
    for r in c_results:
        all_latencies.extend(r[2])

    print(f"\nProducer Results (Total):")
    print(f"  Messages:   {total_p_count}")
    print(f"  Throughput: {total_p_count / total_duration:.2f} msg/s (aggregate)")

    print(f"\nConsumer Results (Total):")
    print(f"  Messages:   {total_c_count}")
    print(f"  Throughput: {total_c_count / total_duration:.2f} msg/s (aggregate)")

    print(f"\nTotal Test Duration: {total_duration:.2f}s")

    if all_latencies:
        print(f"\nLatency Statistics:")
        print(f"  Avg E2E Latency: {statistics.mean(all_latencies) * 1000:.2f}ms")
        print(f"  Min Latency:     {min(all_latencies) * 1000:.2f}ms")
        print(f"  Max Latency:     {max(all_latencies) * 1000:.2f}ms")
        if len(all_latencies) > 1:
            print(
                f"  P95 Latency:     {statistics.quantiles(all_latencies, n=20)[18] * 1000:.2f}ms"
            )

    if server_task and server:
        server.should_exit = True
        await server_task
        print("\nInternal server stopped.")


if __name__ == "__main__":
    asyncio.run(main())
