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
with complete configuration including OAuth, reliability, and performance settings.

This example demonstrates the full configuration options available when
publishing to MSAK using the Pub/Sub Lite client library.
"""

import argparse
import sys
import os

# Add the parent directory to sys.path for development usage
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from google.auth import default
from google.cloud.pubsublite.cloudpubsub.publisher_client import PublisherClient
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import (
    KafkaConfigBuilder,
    create_oauth_token_callback,
)
from google.cloud.pubsublite.types import TopicPath
from google.cloud.pubsub_v1.types import BatchSettings
from datetime import timedelta


def create_full_kafka_config(bootstrap_servers: str, use_oauth: bool = True):
    """
    Create a complete Kafka producer configuration with all recommended settings.
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        use_oauth: If True, use OAuth authentication. If False, use mTLS.
        
    Returns:
        Complete Kafka producer configuration dictionary
    """
    if use_oauth:
        # OAuth/SASL configuration with Google Application Default Credentials
        credentials, _ = default()
        
        # Start with OAuth base configuration
        config = KafkaConfigBuilder.build_oauth_config(
            bootstrap_servers=bootstrap_servers,
            credentials=credentials
        )
    else:
        # mTLS configuration (requires certificates)
        config = KafkaConfigBuilder.build_mtls_config(
            bootstrap_servers=bootstrap_servers,
            cert_location='/path/to/client-cert.pem',
            key_location='/path/to/client-key.pem',
            ca_location='/path/to/ca-chain.pem'
        )
    
    # Add reliability settings for exactly-once semantics
    config = KafkaConfigBuilder.add_reliability_settings(
        config,
        enable_idempotence=True
    )
    
    # Add performance settings with compression
    config = KafkaConfigBuilder.add_performance_settings(
        config,
        compression='snappy'  # or 'gzip', 'lz4', 'zstd'
    )
    
    return config


def publish_with_full_config(
    project_id: str,
    location: str,
    topic_name: str,
    bootstrap_servers: str,
    num_messages: int = 10
):
    """
    Publish messages to a Kafka topic with complete configuration.
    
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
    print(f"Number of messages: {num_messages}")
    print()
    
    # Create batch settings for optimal throughput
    batch_settings = BatchSettings(
        max_messages=100,  # Max messages per batch
        max_bytes=1024 * 1024,  # 1MB max batch size
        max_latency=timedelta(milliseconds=10),  # 10ms max latency
    )
    
    # Get complete Kafka configuration
    kafka_config = create_full_kafka_config(bootstrap_servers)
    
    # Display configuration summary
    print("Configuration Summary:")
    print(f"  Security Protocol: {kafka_config['security.protocol']}")
    print(f"  Idempotence: {kafka_config.get('enable.idempotence', False)}")
    print(f"  Compression: {kafka_config.get('compression.type', 'none')}")
    print(f"  Batch Size: {kafka_config.get('batch.size', 0)} bytes")
    print(f"  Linger MS: {kafka_config.get('linger.ms', 0)}")
    print()
    
    # Create Publisher Client with Kafka backend and full configuration
    client = PublisherClient(
        use_kafka=True,
        kafka_producer_config=kafka_config,
        per_partition_batching_settings=batch_settings,
        enable_idempotence=True  # Enable client-side idempotence tracking
    )
    
    try:
        with client:
            futures = []
            
            # Publish multiple messages
            for i in range(num_messages):
                message_data = f"Message {i+1}: Full configuration example"
                
                # Publish with ordering key and attributes
                future = client.publish(
                    topic=topic_path,
                    data=message_data.encode('utf-8'),
                    ordering_key=f"key-{i % 3}",  # Distribute across 3 partitions
                    # Custom attributes become Kafka headers
                    message_id=str(i+1),
                    timestamp=str(int(time.time())),
                    source="full-config-sample"
                )
                
                futures.append((i+1, future))
                print(f"  → Published message {i+1}")
            
            # Wait for all acknowledgments
            print("\nWaiting for acknowledgments...")
            for msg_num, future in futures:
                try:
                    ack_id = future.result(timeout=30)
                    print(f"  ✓ Message {msg_num} acknowledged: {ack_id}")
                except Exception as e:
                    print(f"  ✗ Message {msg_num} failed: {e}")
            
            print(f"\n✓ Successfully published {len(futures)} messages!")
    
    except Exception as error:
        print(f"\n✗ Failed to publish messages: {error}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Publish messages to MSAK with complete configuration"
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
    print("Google Managed Service for Apache Kafka - Full Configuration Sample")
    print("=" * 70)
    print()
    
    import time
    start_time = time.time()
    
    publish_with_full_config(
        project_id=args.project_id,
        location=args.location,
        topic_name=args.topic_name,
        bootstrap_servers=args.bootstrap_servers,
        num_messages=args.num_messages
    )
    
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 70)
    print(f"Publishing completed in {elapsed_time:.2f} seconds")
    print(f"Throughput: {args.num_messages / elapsed_time:.2f} messages/second")
    

if __name__ == "__main__":
    import time
    main()