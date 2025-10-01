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
Subscribe to messages from a Managed Service for Apache Kafka (MSAK) topic.

This sample demonstrates how to subscribe to and receive messages from
Google Cloud's Managed Service for Apache Kafka using the Pub/Sub Lite
client library's Kafka backend.
"""

import argparse
import logging
import signal
import sys
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from google.auth import default
from google.cloud.pubsublite.cloudpubsub import SubscriberClient
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import (
    KafkaConfig,
    KafkaConfigBuilder,
)
from google.cloud.pubsublite.types import (
    FlowControlSettings,
    SubscriptionPath,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_kafka_config(bootstrap_servers: str, use_oauth: bool = True) -> KafkaConfig:
    """
    Create Kafka configuration for the consumer.

    Args:
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        use_oauth: Whether to use OAuth authentication (vs plaintext)

    Returns:
        KafkaConfig object with appropriate settings
    """
    if use_oauth:
        # Get Google Application Default Credentials
        credentials, _ = default()

        # Build OAuth configuration
        config = KafkaConfigBuilder.build_oauth_config(
            bootstrap_servers=bootstrap_servers,
            credentials=credentials
        )
    else:
        # Simple plaintext configuration for local testing
        config = {
            'bootstrap.servers': bootstrap_servers,
        }

    return KafkaConfig(consumer_config=config)


def message_callback(message):
    """
    Callback function for processing received messages.

    Args:
        message: The received Cloud Pub/Sub message
    """
    try:
        # Decode message data
        data = message.data.decode('utf-8')

        # Log message details
        logger.info(f"Received message:")
        logger.info(f"  Data: {data}")
        logger.info(f"  Ordering key: {message.ordering_key}")
        logger.info(f"  Attributes: {dict(message.attributes)}")
        logger.info(f"  Publish time: {message.publish_time}")
        logger.info(f"  Ack ID: {message.ack_id}")

        # Acknowledge the message
        message.ack()
        logger.debug(f"Message acknowledged: {message.ack_id}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Nack the message to retry later
        message.nack()


def subscribe_with_kafka(
    project_id: str,
    location: str,
    subscription_name: str,
    bootstrap_servers: str,
    consumer_group: Optional[str] = None,
    max_messages: int = 1000,
):
    """
    Subscribe to messages from a Kafka topic.

    Args:
        project_id: Google Cloud project ID
        location: Cloud region (e.g., 'us-central1-a')
        subscription_name: Name of the subscription (maps to Kafka topic)
        bootstrap_servers: Kafka bootstrap servers
        consumer_group: Consumer group ID (optional)
        max_messages: Maximum messages to buffer
    """
    # Create subscription path (we reuse this for topic name)
    subscription = SubscriptionPath(project_id, location, subscription_name)

    # Configure flow control
    flow_control = FlowControlSettings(
        messages_outstanding=max_messages,
        bytes_outstanding=10 * 1024 * 1024,  # 10MB
    )

    # Create Kafka configuration
    kafka_config = create_kafka_config(bootstrap_servers)

    # If no consumer group specified, use a default
    if not consumer_group:
        consumer_group = f"pubsublite-consumer-{subscription_name}"

    logger.info(f"Starting Kafka subscriber:")
    logger.info(f"  Topic: {subscription_name}")
    logger.info(f"  Bootstrap servers: {bootstrap_servers}")
    logger.info(f"  Consumer group: {consumer_group}")
    logger.info(f"  Max messages: {max_messages}")

    # Create subscriber client with Kafka backend
    with ThreadPoolExecutor(max_workers=1) as executor:
        # Note: In a real implementation, we'd need to update SubscriberClient
        # to support use_kafka parameter. For now, we'll use the factory directly.
        from google.cloud.pubsublite.cloudpubsub.internal.make_subscriber import (
            make_async_subscriber
        )
        from google.cloud.pubsublite.cloudpubsub.internal.multiplexed_subscriber_client import (
            MultiplexedSubscriberClient
        )

        # Create the subscriber using our Kafka backend
        subscriber_factory = lambda sub, parts, settings: make_async_subscriber(
            subscription=sub,
            transport="grpc_asyncio",
            per_partition_flow_control_settings=settings,
            nack_handler=None,
            reassignment_handler=None,
            message_transformer=None,
            fixed_partitions=parts,
            credentials=None,
            client_options=None,
            use_kafka=True,
            kafka_config=kafka_config,
            consumer_group=consumer_group,
        )

        subscriber = MultiplexedSubscriberClient(
            executor,
            subscriber_factory
        )

        # Set up signal handler for graceful shutdown
        def signal_handler(sig, frame):
            logger.info("Shutting down subscriber...")
            streaming_pull_future.cancel()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start subscriber
        with subscriber:
            streaming_pull_future = subscriber.subscribe(
                subscription,
                message_callback,
                flow_control,
            )

            logger.info("Subscriber is running. Press Ctrl+C to stop.")

            try:
                # Block until cancelled
                streaming_pull_future.result()
            except KeyboardInterrupt:
                logger.info("Subscriber interrupted by user")
            except Exception as e:
                logger.error(f"Subscriber error: {e}")
                raise
            finally:
                streaming_pull_future.cancel()
                logger.info("Subscriber stopped")


def main():
    parser = argparse.ArgumentParser(
        description="Subscribe to messages from a Managed Service for Apache Kafka topic"
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="Google Cloud project ID"
    )
    parser.add_argument(
        "--location",
        required=True,
        help="Cloud region (e.g., 'us-central1-a')"
    )
    parser.add_argument(
        "--subscription-name",
        required=True,
        help="Name of the subscription (maps to Kafka topic name)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers (e.g., 'localhost:9092')"
    )
    parser.add_argument(
        "--consumer-group",
        help="Consumer group ID (optional, will generate if not provided)"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=1000,
        help="Maximum number of messages to buffer (default: 1000)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )

    args = parser.parse_args()

    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Run subscriber
    subscribe_with_kafka(
        project_id=args.project_id,
        location=args.location,
        subscription_name=args.subscription_name,
        bootstrap_servers=args.bootstrap_servers,
        consumer_group=args.consumer_group,
        max_messages=args.max_messages,
    )


if __name__ == "__main__":
    main()