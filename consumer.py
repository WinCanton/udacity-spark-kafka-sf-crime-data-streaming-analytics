import asyncio
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "Client_0"})
    c.subscribe([topic_name])

    while True:

        messages = c.consume(5, timeout=0.1)
        print(f"Consumed {len(messages)} messages")

        for message in messages:
            if message is None:
                continue

            elif message.error() is not None:
                print(f"An error received: {message.error()}")

            else:
                print(f"Consumed message - Key:{message.key()}, Value: {message.value()}")

        await asyncio.sleep(0.01)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(async_consume("udacity.project.sfcrime.analytics"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def async_consume(topic_name):
    """Runs the Consumer tasks"""
    t1 = asyncio.create_task(consume(topic_name))
    await t1


if __name__ == "__main__":
    main()