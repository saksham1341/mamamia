import asyncio
import random
from mamamia.client.consumer import ConsumerClient


async def main():
    # Connect to local server on port 8000
    consumer = ConsumerClient(
        "http://localhost:8000", log_id="demo-log", group_id="demo-group"
    )

    print(f"Consumer {consumer.client_id} started. Polling for messages...")

    try:
        while True:
            try:
                # Atomically acquire the next available message
                msg = await consumer.acquire_next(duration=10.0)

                if msg:
                    print(f"Acquired message {msg['id']}: {msg['payload']}")

                    # Simulate processing work
                    await asyncio.sleep(random.uniform(0.5, 2.0))

                    # Settle the message
                    await consumer.settle(msg["id"], success=True)
                    print(f"Successfully processed and settled message {msg['id']}")
                else:
                    await asyncio.sleep(2)
            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(5)
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
