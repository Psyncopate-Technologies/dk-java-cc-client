"""Kafka Producer with Microsoft Entra ID OAuth/OIDC authentication."""

from confluent_kafka import Producer
from oauth_callback import oauth_cb
from config import (
    BOOTSTRAP_SERVERS,
    LOGICAL_CLUSTER,
    IDENTITY_POOL_ID,
    TOPIC
)


def delivery_callback(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Produced message to topic {msg.topic()} "
              f"partition {msg.partition()} "
              f"offset {msg.offset()}: "
              f"key = {msg.key().decode('utf-8') if msg.key() else None} "
              f"value = {msg.value().decode('utf-8')}")


def create_producer():
    """Create and return a Kafka producer configured with OAuth."""
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
        "sasl.oauthbearer.config": f"extension_logicalCluster={LOGICAL_CLUSTER} extension_identityPoolId={IDENTITY_POOL_ID}",
        "acks": "all",
    }
    return Producer(config)


def produce_messages(topic: str, message_count: int = 10):
    """Produce test messages to the specified topic."""
    producer = create_producer()

    print(f"Producing {message_count} messages to topic '{topic}'...")

    for i in range(message_count):
        key = f"key-{i}"
        value = f"hello from Python OIDC producer {i}"

        producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=delivery_callback
        )

        # Serve delivery callbacks periodically
        producer.poll(0)

    # Wait for all messages to be delivered
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        print(f"Warning: {remaining} messages were not delivered")
    else:
        print("All messages delivered successfully!")


if __name__ == "__main__":
    produce_messages(TOPIC)
