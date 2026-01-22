import asyncio
import random
import multiprocessing
import time
import uvicorn
from mamamia.server.api import app
from mamamia.client.producer import ProducerClient
from mamamia.client.consumer import ConsumerClient


async def run_producer(log_id: str, count: int):
    producer = ProducerClient("http://localhost:8001", log_id)
    print(f"[Producer] Sending {count} messages...")
    for i in range(count):
        processing_time = random.randint(1, 5)
        await producer.send({"data": f"msg-{i}", "processing_time": processing_time})
    await producer.close()
    print("[Producer] Done.")


async def run_consumer(
    client_id: str, log_id: str, group_id: str, expected: int, results: list
):
    consumer = ConsumerClient("http://localhost:8001", log_id, group_id, client_id)
    print(f"[Consumer {client_id}] Started.")

    while len(results) < expected:
        messages = await consumer.poll(limit=2)
        for msg in messages:
            if await consumer.acquire(msg["id"]):
                payload = msg["payload"]
                proc_time = payload.get("processing_time", 0)
                print(
                    f"[Consumer {client_id}] Processing {msg['id']} for {proc_time}s..."
                )
                await asyncio.sleep(proc_time)

                print(f"[Consumer {client_id}] Finished: {msg['id']}")
                results.append(msg["id"])
                await consumer.settle(msg["id"], success=True)
                if len(results) >= expected:
                    break
        await asyncio.sleep(0.1)
    await consumer.close()
    print(f"[Consumer {client_id}] Finished.")


async def main():
    # Start server in background
    config = uvicorn.Config(app, host="127.0.0.1", port=8001, log_level="error")

    server = uvicorn.Server(config)

    loop = asyncio.get_event_loop()
    server_task = loop.create_task(server.serve())

    await asyncio.sleep(1)  # Wait for server

    log_id = "test-log"
    group_id = "test-group"
    msg_count = 10
    processed_ids = []

    await run_producer(log_id, msg_count)

    # Run two consumers concurrently
    await asyncio.gather(
        run_consumer("C1", log_id, group_id, msg_count, processed_ids),
        run_consumer("C2", log_id, group_id, msg_count, processed_ids),
    )

    print(f"Total processed: {len(processed_ids)}")
    print(f"Unique processed: {len(set(processed_ids))}")

    assert len(processed_ids) == msg_count
    assert len(set(processed_ids)) == msg_count

    server.should_exit = True
    await server_task


if __name__ == "__main__":
    asyncio.run(main())
