"""
Combined Kafka Producer and Consumer Example
Microsoft Entra ID OAuth/OIDC authentication with Confluent Cloud
"""

import sys
import argparse
from producer import produce_messages
from consumer import consume_messages
from config import TOPIC


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Client Example with OAuth/OIDC authentication"
    )
    parser.add_argument(
        "mode",
        choices=["produce", "consume", "both"],
        help="Run mode: produce, consume, or both"
    )
    parser.add_argument(
        "--topic",
        default=TOPIC,
        help=f"Kafka topic (default: {TOPIC})"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of messages to produce (default: 10)"
    )

    args = parser.parse_args()

    if args.mode == "produce":
        produce_messages(args.topic, args.count)
    elif args.mode == "consume":
        consume_messages(args.topic)
    elif args.mode == "both":
        print("=== PRODUCING MESSAGES ===")
        produce_messages(args.topic, args.count)
        print("\n=== CONSUMING MESSAGES ===")
        print("Press Ctrl+C to stop consuming...")
        consume_messages(args.topic)


if __name__ == "__main__":
    main()
