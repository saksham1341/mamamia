import asyncio
from mamamia.client.producer import ProducerClient


async def main():
    # Connect to local TCP server on port 9000
    producer = ProducerClient("localhost:9000", log_id="demo-log")

    print("Sending messages via Binary TCP...")
    for i in range(10):
        msg_id = await producer.send(
            payload={"text": f"Hello world {i}", "value": i},
            metadata={"source": "example-script"},
        )
        print(f"Sent message {i} with ID: {msg_id}")

    await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
