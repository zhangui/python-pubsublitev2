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
Simple batch publishing example for Google Managed Service for Apache Kafka.

This is a simplified version that demonstrates batch publishing without
complex error handling or performance metrics.
"""

import argparse
import sys
import os
import time
from tokenprovider import TokenProvider

# Add the parent directory to sys.path for development usage
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from google.auth import default
from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import (
    KafkaConfigBuilder,
)
from google.cloud.pubsublite.types import TopicPath


def publish_batch_simple(
    project_id: str,
    location: str,
    topic_name: str,
    bootstrap_servers: str,
    num_messages: int = 100
):
    """
    Publish a batch of messages to a Kafka topic.
    
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
    
    print(f"Publishing {num_messages} messages to Kafka topic: {topic_name}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print()
    
    # Get credentials and build OAuth configuration
    credentials, _ = default()
    
    kafka_config = KafkaConfigBuilder.build_oauth_config(
        bootstrap_servers=bootstrap_servers,
        credentials=credentials
    )
    
    # Add reliability settings
    kafka_config = KafkaConfigBuilder.add_reliability_settings(kafka_config)
    
    # Add performance settings for batching
    kafka_config.update({
        'batch.size': 16384,  # 16KB batches
        'linger.ms': 10,  # Wait up to 10ms to batch
        'compression.type': 'snappy',
    })
    
    # Create Publisher Client
    token_provider = TokenProvider()
    client = PublisherClient(
        use_kafka=True,
        kafka_producer_config={
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': token_provider.get_token,
        }
    )
    
    try:
        with client:
            futures = []
            start_time = time.time()
            
            # Publish messages
            print("Publishing messages...")
            for i in range(num_messages):
                message_data = f"Message {i+1}: Batch publishing test"
                
                # Publish the message
                future = client.publish(
                    topic=topic_path,
                    data=message_data.encode('utf-8'),
                    ordering_key=f"key-{i % 5}",  # Distribute across 5 partitions
                    message_id=str(i+1),
                    batch="true"
                )
                
                futures.append(future)
                
                # Print progress every 10 messages
                if (i + 1) % 10 == 0:
                    print(f"  Published {i+1}/{num_messages} messages...")
            
            # Wait for all futures to complete
            print("\nWaiting for acknowledgments...")
            successful = 0
            failed = 0
            
            for i, future in enumerate(futures):
                try:
                    # Get the result (ack_id)
                    if hasattr(future, 'result'):
                        ack_id = future.result(timeout=10)
                    else:
                        ack_id = str(future)
                    successful += 1
                    
                    # Show progress for first few acks
                    if i < 5:
                        print(f"  Message {i+1} ack: {ack_id}")
                    elif i == 5:
                        print(f"  ... (showing first 5 acks only)")
                        
                except Exception as e:
                    failed += 1
                    print(f"  Message {i+1} failed: {e}")
            
            elapsed_time = time.time() - start_time
            
            print()
            print("=" * 60)
            print("Results:")
            print(f"  Successfully published: {successful}/{num_messages}")
            if failed > 0:
                print(f"  Failed: {failed}")
            print(f"  Total time: {elapsed_time:.2f} seconds")
            print(f"  Throughput: {successful / elapsed_time:.0f} messages/second")
            print()
            print("✓ Batch publishing completed!")
    
    except Exception as error:
        print(f"\n✗ Error: {error}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Simple batch publishing to Google Managed Service for Apache Kafka"
    )
    parser.add_argument(
        "--project-id",
        default="ygnahz-eg-codelab",
        help="Google Cloud project ID"
    )
    parser.add_argument(
        "--location",
        default="us-central1",
        help="Google Cloud location (e.g., us-central1-a)"
    )
    parser.add_argument(
        "--topic-name",
        default="testtopic",
        help="Name of the Kafka topic"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092",
        help="Comma-separated list of Kafka bootstrap servers"
    )
    parser.add_argument(
        "--num-messages",
        type=int,
        default=100,
        help="Number of messages to publish (default: 100)"
    )
    
    args = parser.parse_args()
    
    publish_batch_simple(
        project_id=args.project_id,
        location=args.location,
        topic_name=args.topic_name,
        bootstrap_servers=args.bootstrap_servers,
        num_messages=args.num_messages
    )


if __name__ == "__main__":
    main()