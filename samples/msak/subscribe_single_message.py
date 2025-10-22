#!/usr/bin/env python

# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Sample for subscribing to messages from Google Managed Service for Apache Kafka
using the Universal Subscriber Client.

This example demonstrates subscribing to a Kafka topic using the same
interface as Pub/Sub Lite, with automatic backend switching.
"""

import argparse
import sys
import os
import signal
import time
from tokenprovider import TokenProvider

# Add the parent directory to sys.path for development usage
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from google.cloud.pubsublite.cloudpubsub import SubscriberClient
from google.cloud.pubsublite.types import SubscriptionPath, FlowControlSettings


# Global counter for messages received
messages_received = 0


def message_callback(message):
    """
    Callback function for processing received messages.

    Args:
        message: The received Cloud Pub/Sub message
    """
    global messages_received
    messages_received += 1

    try:
        # Decode and print message
        data = message.data.decode('utf-8')
        print(f"\n[Message {messages_received}]")
        print(f"  Data: {data}")
        if message.ordering_key:
            print(f"  Ordering key: {message.ordering_key}")
        if message.attributes:
            print(f"  Attributes: {dict(message.attributes)}")
        print(f"  Ack ID: {message.ack_id}")

        # Acknowledge the message
        message.ack()

    except Exception as e:
        print(f"✗ Error processing message: {e}")
        message.nack()


def subscribe_messages(
    project_id: str,
    location: str,
    subscription_name: str,
    bootstrap_servers: str,
    consumer_group: str = None,
    duration_seconds: int = 60,
):
    """
    Subscribe to messages from a Kafka topic using the Universal Subscriber Client.

    Args:
        project_id: Google Cloud project ID
        location: Google Cloud location (e.g., "us-central1-a")
        subscription_name: Name of the subscription (maps to Kafka topic)
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        consumer_group: Consumer group ID (optional)
        duration_seconds: How long to run the subscriber (0 = run indefinitely)
    """

    # Create subscription path
    subscription = SubscriptionPath(
        project=project_id,
        location=location,
        name=subscription_name
    )

    print(f"Subscribing to Kafka topic: {subscription_name}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    if consumer_group:
        print(f"Consumer group: {consumer_group}")
    else:
        print(f"Consumer group: pubsublite-{subscription_name} (auto-generated)")
    print(f"Duration: {duration_seconds}s" if duration_seconds > 0 else "Duration: indefinite (press Ctrl+C to stop)")

    # Configure TokenProvider for OAuth
    token_provider = TokenProvider()
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,

        # Consumer config
        'group.id': consumer_group,
        'auto.offset.reset': 'latest',
        'enable.partition.eof': True,  # surface EOF events per partition
        # 'enable.auto.commit': True,  # default True; uncomment to be explicit
    }

    # Configure flow control
    flow_control = FlowControlSettings(
        messages_outstanding=1000,
        bytes_outstanding=10 * 1024 * 1024,  # 10MB
    )

    print(f"\nUsing backend: kafka")
    print(f"Waiting for messages...\n")

    # Configure and create Subscriber Client with Kafka backend
    client = SubscriberClient(
        use_kafka=True,
        kafka_consumer_config=consumer_config,
        consumer_group=consumer_group,
    )

    # Set up signal handler for graceful shutdown
    streaming_pull_future = None

    def signal_handler(sig, frame):
        print("\n\nShutting down subscriber...")
        if streaming_pull_future:
            streaming_pull_future.cancel()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start subscriber
    try:
        with client:
            streaming_pull_future = client.subscribe(
                subscription,
                message_callback,
                flow_control,
            )

            # Run for specified duration or indefinitely
            if duration_seconds > 0:
                time.sleep(duration_seconds)
                print(f"\n\nDuration expired. Received {messages_received} messages.")
                streaming_pull_future.cancel()
            else:
                # Block until cancelled
                print("Subscriber running. Press Ctrl+C to stop.")
                streaming_pull_future.result()

    except KeyboardInterrupt:
        print(f"\n\nSubscriber interrupted. Received {messages_received} messages.")
    except Exception as error:
        print(f"\n✗ Subscriber error: {error}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Subscribe to messages from Google Managed Service for Apache Kafka"
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="Google Cloud project ID"
    )
    parser.add_argument(
        "--location",
        required=True,
        help="Google Cloud location (e.g., us-central1-a)"
    )
    parser.add_argument(
        "--subscription-name",
        required=True,
        help="Name of the subscription (maps to Kafka topic)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Comma-separated list of Kafka bootstrap servers"
    )
    parser.add_argument(
        "--consumer-group",
        help="Consumer group ID (optional, will be auto-generated if not provided)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="How long to run the subscriber in seconds (0 = run indefinitely)"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Google Managed Service for Apache Kafka - Subscribe Messages")
    print("=" * 60)

    subscribe_messages(
        project_id=args.project_id,
        location=args.location,
        subscription_name=args.subscription_name,
        bootstrap_servers=args.bootstrap_servers,
        consumer_group=args.consumer_group,
        duration_seconds=args.duration,
    )

    print("\n" + "=" * 60)
    print(f"Subscriber completed. Total messages: {messages_received}")


if __name__ == "__main__":
    main()
