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
Sample for publishing a single message to Google Managed Service for Apache Kafka
using the Universal Publisher Client.

This example demonstrates publishing a message to a Kafka topic using the same
interface as Pub/Sub Lite, with automatic backend switching.
"""

import argparse
import sys
import os

# Add the parent directory to sys.path for development usage
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from google.cloud.pubsublite.cloudpubsub.universal_client import (
    UniversalPublisherClient,
    create_kafka_config,
)
from google.cloud.pubsublite.types import TopicPath
from google.auth import default


def publish_single_message(
    project_id: str,
    location: str,
    topic_name: str,
    bootstrap_servers: str,
    auth_endpoint: str = "localhost:14293",
    message: str = "Hello from Google Managed Service for Apache Kafka!"
):
    """
    Publish a single message to a Kafka topic using the Universal Publisher Client.
    
    Args:
        project_id: Google Cloud project ID
        location: Google Cloud location (e.g., "us-central1-a") 
        topic_name: Name of the Kafka topic
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        auth_endpoint: OAuth authentication endpoint
        message: Message content to publish
    """
    
    # Create TopicPath in Pub/Sub Lite format (gets converted to Kafka topic internally)
    topic_path = TopicPath(
        project=project_id,
        location=location, 
        name=topic_name
    )
    
    print(f"Publishing to Kafka topic: {topic_name}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Message: {message}")
    
    # Configure Kafka backend
    servers_list = [server.strip() for server in bootstrap_servers.split(',')]
    kafka_config = create_kafka_config(
        bootstrap_servers=servers_list,
        auth_endpoint=auth_endpoint,
        credentials=default()[0],  # Use Application Default Credentials
    )
    
    # Create Universal Publisher Client with Kafka backend
    client = UniversalPublisherClient(
        use_kafka=True,
        kafka_config=kafka_config
    )
    
    print(f"Using backend: {client.backend_type}")
    
    try:
        with client:
            # Publish the message
            future = client.publish(
                topic=topic_path,
                data=message.encode('utf-8'),
                # Optional: add ordering key for partition assignment
                ordering_key="sample-key",
                # Optional: add custom attributes as Kafka headers
                sample_attr="sample_value"
            )
            
            # Wait for acknowledgment
            ack_id = future.result(timeout=30)
            print(f"✓ Message published successfully!")
            print(f"  Ack ID: {ack_id}")
            
            # Parse Kafka ack ID (format: topic:partition:offset)
            if ":" in ack_id:
                topic, partition, offset = ack_id.split(":")
                print(f"  Topic: {topic}")
                print(f"  Partition: {partition}")
                print(f"  Offset: {offset}")
    
    except Exception as error:
        print(f"✗ Failed to publish message: {error}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Publish a single message to Google Managed Service for Apache Kafka"
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
        "--topic-name",
        required=True,
        help="Name of the Kafka topic"
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Comma-separated list of Kafka bootstrap servers (e.g., server1:9092,server2:9092)"
    )
    parser.add_argument(
        "--auth-endpoint",
        default="localhost:14293",
        help="OAuth authentication endpoint (default: localhost:14293)"
    )
    parser.add_argument(
        "--message",
        default="Hello from Google Managed Service for Apache Kafka!",
        help="Message content to publish"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Google Managed Service for Apache Kafka - Publish Message")
    print("=" * 60)
    
    publish_single_message(
        project_id=args.project_id,
        location=args.location,
        topic_name=args.topic_name,
        bootstrap_servers=args.bootstrap_servers,
        auth_endpoint=args.auth_endpoint,
        message=args.message
    )
    
    print("\n" + "=" * 60)
    print("Message publishing completed successfully!")
    

if __name__ == "__main__":
    main()