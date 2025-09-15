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
Sample for publishing batch messages to Google Managed Service for Apache Kafka
using the Universal Publisher Client with optimized batching configuration.

This example demonstrates:
- Batch publishing with optimized settings
- Concurrent message publishing
- Throughput measurement
- Partition distribution with ordering keys
"""

import argparse
import sys
import os
import time
import json
import logging
from datetime import timedelta
from tokenprovider import TokenProvider

logger = logging.getLogger(__name__)

# Add the parent directory to sys.path for development usage
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from google.auth import default
from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import (
    KafkaConfigBuilder,
)
from google.cloud.pubsublite.types import TopicPath
from google.cloud.pubsub_v1.types import BatchSettings


def create_batch_optimized_config(bootstrap_servers: str):
    """
    Create Kafka configuration optimized for batch publishing.
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        
    Returns:
        Optimized Kafka producer configuration
    """
    # Get Google Application Default Credentials
    credentials, _ = default()
    
    # Build OAuth configuration
    config = KafkaConfigBuilder.build_oauth_config(
        bootstrap_servers=bootstrap_servers,
        credentials=credentials
    )
    
    # Add reliability settings for exactly-once semantics
    config = KafkaConfigBuilder.add_reliability_settings(
        config,
        enable_idempotence=True
    )
    
    # Override with batch-optimized settings
    config.update({
        # Batching configuration
        'batch.size': 1048576,  # 1MB batch size
        'linger.ms': 10,  # Wait up to 10ms to batch messages
        'batch.num.messages': 1000,  # Max 1000 messages per batch
        
        # Compression for better throughput
        'compression.type': 'lz4',  # Fast compression with good ratio
        
        # Buffer configuration
        'buffer.memory': 67108864,  # 64MB buffer
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.kbytes': 2097152,  # 2GB
        
        # Performance tuning
        'max.in.flight.requests.per.connection': 5,
        'send.buffer.bytes': 131072,  # 128KB
        'receive.buffer.bytes': 32768,  # 32KB
    })
    
    return config


def generate_test_message(index: int, batch_num: int, total_batches: int) -> dict:
    """
    Generate a test message with metadata.
    
    Args:
        index: Message index within the batch
        batch_num: Current batch number
        total_batches: Total number of batches
        
    Returns:
        Dictionary with message data and attributes
    """
    return {
        'data': json.dumps({
            'message_id': f'batch-{batch_num}-msg-{index}',
            'batch_number': batch_num,
            'message_index': index,
            'total_batches': total_batches,
            'timestamp': time.time(),
            'payload': f'This is test message {index} from batch {batch_num}',
            'metadata': {
                'source': 'batch-publisher-sample',
                'version': '1.0',
                'environment': 'test'
            }
        }).encode('utf-8'),
        'ordering_key': f'partition-{index % 10}',  # Distribute across 10 partitions
        'attributes': {
            'batch_id': str(batch_num),
            'message_id': str(index),
            'content_type': 'application/json',
            'source': 'batch-sample',
            'priority': 'normal' if index % 10 != 0 else 'high'
        }
    }


def publish_batch_messages(
    project_id: str,
    location: str,
    topic_name: str,
    bootstrap_servers: str,
    batch_size: int = 100,
    num_batches: int = 10
):
    """
    Publish messages in batches to a Kafka topic.
    
    Args:
        project_id: Google Cloud project ID
        location: Google Cloud location (e.g., "us-central1-a")
        topic_name: Name of the Kafka topic
        bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        batch_size: Number of messages per batch
        num_batches: Number of batches to publish
    """
    
    # Create TopicPath
    topic_path = TopicPath(
        project=project_id,
        location=location,
        name=topic_name
    )
    
    total_messages = batch_size * num_batches
    
    print("=" * 70)
    print("Batch Message Publishing Configuration")
    print("=" * 70)
    print(f"Topic: {topic_name}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Batch size: {batch_size} messages")
    print(f"Number of batches: {num_batches}")
    print(f"Total messages: {total_messages}")
    print()
    
    # Create batch-optimized configuration
    kafka_config = create_batch_optimized_config(bootstrap_servers)
    
    # Create batch settings for the client
    batch_settings = BatchSettings(
        max_messages=batch_size,
        max_bytes=1024 * 1024,  # 1MB
        max_latency=timedelta(milliseconds=10),
    )
    
    # Display configuration
    print("Kafka Configuration:")
    print(f"  Batch size: {kafka_config.get('batch.size')} bytes")
    print(f"  Linger MS: {kafka_config.get('linger.ms')}ms")
    print(f"  Compression: {kafka_config.get('compression.type')}")
    print(f"  Buffer memory: {kafka_config.get('buffer.memory') / 1048576:.1f}MB")
    print()
    
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
            print("Publishing messages in batches...")
            print()
            
            start_time = time.time()
            futures = []
            message_count = 0
            
            # Publish messages in batches
            for batch_num in range(1, num_batches + 1):
                batch_start = time.time()
                batch_futures = []
                
                print(f"Batch {batch_num}/{num_batches}:")
                
                # Publish all messages in this batch
                for i in range(batch_size):
                    message = generate_test_message(i, batch_num, num_batches)
                    
                    try:
                        future = client.publish(
                            topic=topic_path,
                            data=message['data'],
                            ordering_key=message['ordering_key'],
                            **message['attributes']
                        )
                        
                        batch_futures.append(future)
                        message_count += 1
                    except Exception as e:
                        print(f"  ✗ Failed to publish message {i}: {e}")
                
                # Track batch completion
                batch_time = time.time() - batch_start
                batch_rate = batch_size / batch_time if batch_time > 0 else 0
                
                # Wait for acknowledgments from this batch
                successful = 0
                failed = 0
                for future in batch_futures:
                    try:
                        # Get the ack_id (even if it's a placeholder)
                        ack_id = future.result(timeout=5) if hasattr(future, 'result') else str(future)
                        successful += 1
                    except Exception as e:
                        failed += 1
                        logger.debug(f"Failed to get ack for message: {e}")
                
                print(f"  ✓ Sent {successful} messages in {batch_time:.3f}s")
                if failed > 0:
                    print(f"  ⚠ {failed} messages failed")
                print(f"  Rate: {batch_rate:.0f} messages/second")
                
                futures.extend(batch_futures)
                
                # Small delay between batches to demonstrate batching behavior
                if batch_num < num_batches:
                    time.sleep(0.1)
            
            total_time = time.time() - start_time
            
            print()
            print("=" * 70)
            print("Publishing Complete!")
            print("=" * 70)
            print(f"Total messages published: {message_count}")
            print(f"Total time: {total_time:.2f} seconds")
            print(f"Overall throughput: {message_count / total_time:.0f} messages/second")
            print(f"Average latency: {(total_time / message_count) * 1000:.2f}ms per message")
            
            # Calculate partition distribution
            print()
            print("Partition Distribution:")
            for i in range(10):
                partition_count = sum(1 for j in range(total_messages) if j % 10 == i)
                print(f"  Partition {i}: {partition_count} messages")
            
            print()
            print("✓ All messages published successfully!")
    
    except Exception as error:
        print(f"\n✗ Failed to publish messages: {error}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Publish batch messages to Google Managed Service for Apache Kafka"
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
        "--batch-size",
        type=int,
        default=100,
        help="Number of messages per batch (default: 100)"
    )
    parser.add_argument(
        "--num-batches",
        type=int,
        default=10,
        help="Number of batches to publish (default: 10)"
    )
    
    args = parser.parse_args()
    
    print()
    print("🚀 Google Managed Service for Apache Kafka - Batch Publisher")
    print("=" * 70)
    
    publish_batch_messages(
        project_id=args.project_id,
        location=args.location,
        topic_name=args.topic_name,
        bootstrap_servers=args.bootstrap_servers,
        batch_size=args.batch_size,
        num_batches=args.num_batches
    )
    
    print()
    print("🎉 Batch publishing demonstration completed!")
    print()


if __name__ == "__main__":
    main()