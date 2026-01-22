import asyncio
from mamamia.client.producer import ProducerClient


async def main():
    # Connect to local server on port 8000
    producer = ProducerClient("http://localhost:8000", log_id="demo-log")

    print("Sending messages...")
    for i in range(10):
        msg_id = await producer.send(
            payload={"text": f"Hello world {i}", "value": i},
            metadata={"source": "example-script"},
        )
        print(f"Sent message {i} with ID: {msg_id}")

    await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
