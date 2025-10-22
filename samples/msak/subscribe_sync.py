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
Subscribe to messages from Managed Service for Apache Kafka (MSAK) using SubscriberServiceClient.

This sample demonstrates subscribing to Google Cloud's Managed Service for Apache Kafka
using the Pub/Sub Lite v1 SubscriberServiceClient with Kafka transport and TokenProvider
for OAuth authentication.
"""

import logging
import threading
import time
import signal
import sys
from typing import Optional
from google.cloud import pubsublite_v1

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
    from tokenprovider import TokenProvider
except ImportError:
    TokenProvider = None


class KafkaSubscriber:
    """Manages bidirectional streaming for Kafka subscription."""

    def __init__(
        self,
        bootstrap_servers: str,
        subscription_name: str,
        project_id: str,
        location: str,
        partition: int = 0,
        consumer_group: Optional[str] = None,
    ):
        """Initialize the Kafka subscriber.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            subscription_name: Subscription/topic name
            project_id: Google Cloud project ID
            location: Cloud location
            partition: Partition to subscribe to
            consumer_group: Consumer group ID (optional)
        """
        if TokenProvider is None:
            raise ImportError("TokenProvider not available")

        self.subscription_name = subscription_name
        self.partition = partition
        self.consumer_group = consumer_group or f"pubsublite-{subscription_name}-p{partition}"

        # Create Kafka config with OAuth authentication
        token_provider = TokenProvider()
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': token_provider.get_token,
            'group.id': self.consumer_group,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,  # Manual commit on ack
        }

        # Create client with Kafka transport
        self.client = pubsublite_v1.SubscriberServiceClient(
            transport="kafka",
            consumer_config=self.consumer_config,
        )

        # Construct subscription path
        self.subscription_path = f"projects/{project_id}/locations/{location}/subscriptions/{subscription_name}"

        # Control flags
        self.running = True
        self.flow_control_sent = threading.Event()
        self.messages_received = 0

    def request_generator(self):
        """Generate subscribe requests for bidirectional streaming.

        Yields:
            SubscribeRequest messages (initial, flow_control)
        """
        # Send initial request
        print(f"Subscribing to {self.subscription_name} partition {self.partition}")
        yield pubsublite_v1.SubscribeRequest(
            initial=pubsublite_v1.InitialSubscribeRequest(
                subscription=self.subscription_path,
                partition=self.partition,
            )
        )

        # Wait a bit for initial response
        time.sleep(0.5)

        # Continuously send flow control requests
        while self.running:
            # Grant flow control tokens
            yield pubsublite_v1.SubscribeRequest(
                flow_control=pubsublite_v1.FlowControlRequest(
                    allowed_messages=100,  # Allow up to 100 messages
                    allowed_bytes=1024 * 1024,  # Allow up to 1MB
                )
            )
            self.flow_control_sent.set()

            # Wait before sending next flow control
            time.sleep(1.0)

    def subscribe(self, duration_seconds: int = 60):
        """Subscribe and process messages for specified duration.

        Args:
            duration_seconds: How long to run the subscriber (0 = indefinite)
        """
        # Set up signal handler for graceful shutdown
        def signal_handler(_sig, _frame):
            print("\n\nShutting down subscriber...")
            self.running = False
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        print(f"Starting subscriber (duration: {duration_seconds}s, consumer group: {self.consumer_group})")
        print("Waiting for messages...\n")

        # Start subscription stream
        stream = self.client.subscribe(requests=self.request_generator())

        # Track start time for duration limit
        start_time = time.time()

        try:
            for response in stream:
                # Check duration limit
                if duration_seconds > 0 and time.time() - start_time > duration_seconds:
                    print(f"\nDuration expired. Received {self.messages_received} messages.")
                    self.running = False
                    break

                # Handle different response types
                if response.initial:
                    print(f"Initial response - cursor: {response.initial.cursor}")

                elif response.messages:
                    # Process message batch
                    for message in response.messages.messages:
                        self.messages_received += 1

                        # Decode message data
                        data = message.message.data.decode('utf-8', errors='ignore')

                        # Display message
                        print(f"[Message {self.messages_received}]")
                        print(f"  Data: {data}")
                        print(f"  Offset: {message.cursor.offset}")

                        if message.message.key:
                            print(f"  Key: {message.message.key.decode('utf-8', errors='ignore')}")

                        if message.message.attributes:
                            print(f"  Attributes: {dict(message.message.attributes)}")

                        print(f"  Size: {message.size_bytes} bytes")
                        print(f"  Publish time: {message.publish_time}")
                        print()

                elif response.seek:
                    print(f"Seek response - new cursor: {response.seek.cursor}")

        except Exception as e:
            print(f"Error in subscription stream: {e}")
        finally:
            self.running = False
            print(f"\nSubscriber stopped. Total messages received: {self.messages_received}")


def subscribe_messages_kafka(
    bootstrap_servers: str,
    subscription_name: str,
    project_id: str,
    location: str,
    partition: int = 0,
    consumer_group: Optional[str] = None,
    duration_seconds: int = 60,
):
    """Subscribe to messages from Kafka using SubscriberServiceClient.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        subscription_name: Subscription/topic name
        project_id: Google Cloud project ID
        location: Cloud location
        partition: Partition to subscribe to
        consumer_group: Consumer group ID (optional)
        duration_seconds: How long to run (0 = indefinite)
    """
    subscriber = KafkaSubscriber(
        bootstrap_servers=bootstrap_servers,
        subscription_name=subscription_name,
        project_id=project_id,
        location=location,
        partition=partition,
        consumer_group=consumer_group,
    )

    subscriber.subscribe(duration_seconds=duration_seconds)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Subscribe from MSAK using Kafka transport")
    parser.add_argument(
        "--bootstrap-servers",
        default="bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument("--subscription", default="testtopic", help="Subscription/topic name")
    parser.add_argument("--project-id", default="ygnahz-eg-codelab", help="Google Cloud project ID")
    parser.add_argument("--location", default="us-central1", help="Cloud location")
    parser.add_argument("--partition", type=int, default=2, help="Partition to subscribe from")
    parser.add_argument("--consumer-group", help="Consumer group ID (optional)")
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="How long to run in seconds (0 = indefinite)"
    )

    args = parser.parse_args()

    subscribe_messages_kafka(
        bootstrap_servers=args.bootstrap_servers,
        subscription_name=args.subscription,
        project_id=args.project_id,
        location=args.location,
        partition=args.partition,
        consumer_group=args.consumer_group,
        duration_seconds=args.duration,
    )