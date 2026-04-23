"""Kafka Consumer with Microsoft Entra ID OAuth/OIDC authentication."""

import signal
import sys
from confluent_kafka import Consumer, KafkaException
from oauth_callback import oauth_cb
from config import (
    BOOTSTRAP_SERVERS,
    LOGICAL_CLUSTER,
    IDENTITY_POOL_ID,
    TOPIC,
    GROUP_ID
)

# Global flag for graceful shutdown
running = True


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    global running
    print("\nShutdown signal received; closing consumer.")
    running = False


def create_consumer():
    """Create and return a Kafka consumer configured with OAuth."""
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
        "sasl.oauthbearer.config": f"extension_logicalCluster={LOGICAL_CLUSTER} extension_identityPoolId={IDENTITY_POOL_ID}",
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    return Consumer(config)


def consume_messages(topic: str):
    """Consume messages from the specified topic."""
    consumer = create_consumer()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        consumer.subscribe([topic])
        print(f"Subscribed to topic '{topic}'. Waiting for messages...")

        while running:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                print("No records yet.")
                continue

            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8") if msg.value() else None

            print(f"Consumed message from topic {msg.topic()}: "
                  f"key = {key} value = {value}")

    except KafkaException as e:
        print(f"Kafka error: {e}")
        sys.exit(1)
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    consume_messages(TOPIC)
