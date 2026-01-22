import asyncio
import random
import logging
from mamamia.server.registry import LogRegistry
from mamamia.server.tcp import TcpFrontend
from mamamia.client.producer import ProducerClient
from mamamia.client.consumer import ConsumerClient


async def run_producer(addr: str, log_id: str, count: int):
    producer = ProducerClient(addr, log_id)
    print(f"[Producer] Sending {count} messages...")
    for i in range(count):
        processing_time = random.randint(1, 5)
        await producer.send({"data": f"msg-{i}", "processing_time": processing_time})
    await producer.close()
    print("[Producer] Done.")


async def run_consumer(
    client_id: str, addr: str, log_id: str, group_id: str, expected: int, results: list
):
    consumer = ConsumerClient(addr, log_id, group_id, client_id)
    print(f"[Consumer {client_id}] Started.")

    while len(results) < expected:
        msg = await consumer.acquire_next(duration=30.0)
        if msg:
            payload = msg["payload"]
            proc_time = payload.get("processing_time", 0)
            print(f"[Consumer {client_id}] Processing {msg['id']} for {proc_time}s...")
            await asyncio.sleep(proc_time)

            print(f"[Consumer {client_id}] Finished: {msg['id']}")
            results.append(msg["id"])
            await consumer.settle(msg["id"], success=True)
            if len(results) >= expected:
                break
        else:
            await asyncio.sleep(0.1)
    await consumer.close()
    print(f"[Consumer {client_id}] Finished.")


async def main():
    # Setup server
    registry = LogRegistry()
    registry.start_reaper(interval=10.0)
    server = TcpFrontend(registry, host="127.0.0.1", port=9001)

    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(1)  # Wait for server

    addr = "127.0.0.1:9001"
    log_id = "test-log"
    group_id = "test-group"
    msg_count = 10
    processed_ids = []

    await run_producer(addr, log_id, msg_count)

    # Run two consumers concurrently
    await asyncio.gather(
        run_consumer("C1", addr, log_id, group_id, msg_count, processed_ids),
        run_consumer("C2", addr, log_id, group_id, msg_count, processed_ids),
    )

    print(f"Total processed: {len(processed_ids)}")
    print(f"Unique processed: {len(set(processed_ids))}")

    assert len(processed_ids) == msg_count
    assert len(set(processed_ids)) == msg_count

    await server.stop()
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
