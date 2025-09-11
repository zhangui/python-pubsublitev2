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
Sample for publishing messages to Google Managed Service for Apache Kafka
using OAuth/SASL authentication with Google Application Default Credentials.

This example demonstrates the complete OAuth configuration with reliability
and performance settings.
"""

import argparse
import sys
import os
import time

# Add the parent directory to sys.path for development usage
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from google.auth import default
from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import (
    KafkaConfigBuilder,
)
from google.cloud.pubsublite.types import TopicPath
from google.cloud.pubsub_v1.types import BatchSettings
from datetime import timedelta


def publish_with_oauth(
    project_id: str,
    location: str,
    topic_name: str,
    bootstrap_servers: str,
    num_messages: int = 10
):
    """
    Publish messages to a Kafka topic using OAuth/SASL authentication.
    
    Args:
        project_id: Google Cloud project ID
        location: Google Cloud location (e.g., "us-central1-a")
        topic_name: Name of the Kafka topic
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        num_messages: Number of messages to publish
    """
    
    # Create TopicPath
    topic_path = TopicPath(
        project=project_id,
        location=location,
        name=topic_name
    )
    
    print(f"Publishing to Kafka topic: {topic_name}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Authentication: OAuth/SASL with Google ADC")
    print(f"Number of messages: {num_messages}")
    print()
    
    # Get Google Application Default Credentials
    credentials, _ = default()
    
    # Build OAuth configuration with reliability and performance settings
    kafka_config = KafkaConfigBuilder.build_oauth_config(
        bootstrap_servers=bootstrap_servers,
        credentials=credentials
    )
    
    # Add reliability settings for exactly-once semantics
    kafka_config = KafkaConfigBuilder.add_reliability_settings(
        kafka_config,
        enable_idempotence=True
    )
    
    # Add performance settings with compression
    kafka_config = KafkaConfigBuilder.add_performance_settings(
        kafka_config,
        compression='snappy'  # or 'gzip', 'lz4', 'zstd'
    )
    
    # Create batch settings for optimal throughput
    batch_settings = BatchSettings(
        max_messages=100,  # Max messages per batch
        max_bytes=1024 * 1024,  # 1MB max batch size
        max_latency=timedelta(milliseconds=10),  # 10ms max latency
    )
    
    # Display configuration summary
    print("Configuration Summary:")
    print(f"  Security Protocol: {kafka_config['security.protocol']}")
    print(f"  SASL Mechanism: {kafka_config['sasl.mechanisms']}")
    print(f"  Idempotence: {kafka_config.get('enable.idempotence', False)}")
    print(f"  Acknowledgments: {kafka_config.get('acks', 'none')}")
    print(f"  Compression: {kafka_config.get('compression.type', 'none')}")
    print(f"  Batch Size: {kafka_config.get('batch.size', 0)} bytes")
    print(f"  Linger MS: {kafka_config.get('linger.ms', 0)}")
    print()
    
    # Create Publisher Client with Kafka backend
    client = PublisherClient(
        use_kafka=True,
        kafka_producer_config=kafka_config,
        per_partition_batching_settings=batch_settings,
        credentials=credentials,
        enable_idempotence=True  # Enable client-side idempotence tracking
    )
    
    try:
        with client:
            start_time = time.time()
            
            # Publish multiple messages
            for i in range(num_messages):
                message_data = f"OAuth Message {i+1}: Authenticated with Google ADC"
                
                # Publish with ordering key and attributes
                future = client.publish(
                    topic=topic_path,
                    data=message_data.encode('utf-8'),
                    ordering_key=f"key-{i % 3}",  # Distribute across 3 partitions
                    # Custom attributes become Kafka headers
                    message_id=str(i+1),
                    timestamp=str(int(time.time())),
                    source="oauth-sample",
                    auth_method="OAUTHBEARER"
                )
                
                # Note: Without Futures, we get a placeholder ack_id immediately
                ack_id = future.result() if hasattr(future, 'result') else future
                print(f"  → Published message {i+1}: {ack_id}")
            
            elapsed_time = time.time() - start_time
            
            print(f"\n✓ Successfully published {num_messages} messages!")
            print(f"  Total time: {elapsed_time:.2f} seconds")
            print(f"  Throughput: {num_messages / elapsed_time:.2f} messages/second")
    
    except Exception as error:
        print(f"\n✗ Failed to publish messages: {error}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Publish messages to MSAK with OAuth/SASL authentication"
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
        help="Comma-separated list of Kafka bootstrap servers"
    )
    parser.add_argument(
        "--num-messages",
        type=int,
        default=10,
        help="Number of messages to publish (default: 10)"
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("Google Managed Service for Apache Kafka - OAuth/SASL Authentication")
    print("=" * 70)
    print()
    
    publish_with_oauth(
        project_id=args.project_id,
        location=args.location,
        topic_name=args.topic_name,
        bootstrap_servers=args.bootstrap_servers,
        num_messages=args.num_messages
    )
    

if __name__ == "__main__":
    main()