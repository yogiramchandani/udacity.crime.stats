import asyncio

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_URL = "udacity.project.2.crime"

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(2.5)

    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        }
    )

    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(0.1)


def on_assign(consumer, partitions):
    """Callback for when topic assignment takes place"""
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING

    consumer.assign(partitions)


def main():
    """Runs the exercise"""
    try:
         asyncio.run(consume(TOPIC_URL))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()