#!/usr/bin/env python3

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
Example demonstrating the Universal Publisher Client.

This example shows how to use the UniversalPublisherClient to publish
messages to either Pub/Sub Lite or Google Managed Service for Apache Kafka
with the same API.
"""

import os
from google.cloud.pubsublite.cloudpubsub.universal_client import (
    UniversalPublisherClient,
    create_kafka_config,
)
from google.cloud.pubsublite.types import TopicPath


def example_pubsublite_backend():
    """Example using Pub/Sub Lite backend (default)."""
    print("=== Pub/Sub Lite Backend Example ===")
    
    topic_path = TopicPath.parse(
        "projects/your-project/locations/us-central1-a/topics/your-topic"
    )
    
    # Use Pub/Sub Lite backend (default)
    client = UniversalPublisherClient()
    print(f"Backend type: {client.backend_type}")
    
    with client:
        # Publish a message
        future = client.publish(
            topic=topic_path,
            data=b"Hello from Pub/Sub Lite!",
            ordering_key="key1",
            custom_attr="value1"
        )
        
        ack_id = future.result()
        print(f"Message published with ack ID: {ack_id}")


def example_kafka_backend():
    """Example using Kafka backend."""
    print("\n=== Kafka Backend Example ===")
    
    topic_path = TopicPath.parse(
        "projects/your-project/locations/us-central1-a/topics/your-kafka-topic"
    )
    
    # Configure Kafka backend
    kafka_config = create_kafka_config(
        bootstrap_servers=[
            "your-cluster-bootstrap-server-1:9092",
            "your-cluster-bootstrap-server-2:9092",
        ],
        auth_endpoint="localhost:14293",  # Local OAuth server
    )
    
    # Use Kafka backend explicitly
    client = UniversalPublisherClient(
        use_kafka=True,
        kafka_config=kafka_config
    )
    print(f"Backend type: {client.backend_type}")
    
    with client:
        # Publish a message (same API!)
        future = client.publish(
            topic=topic_path,
            data=b"Hello from Kafka!",
            ordering_key="key1", 
            custom_attr="value1"
        )
        
        ack_id = future.result()  # Format: topic:partition:offset
        print(f"Message published with ack ID: {ack_id}")


def example_environment_variable():
    """Example using environment variable to switch backends."""
    print("\n=== Environment Variable Example ===")
    
    topic_path = TopicPath.parse(
        "projects/your-project/locations/us-central1-a/topics/your-topic"
    )
    
    # Set environment variable to use Kafka
    # In practice, you would set this before running your application:
    # export PUBSUBLITE_USE_KAFKA=true
    # export KAFKA_BOOTSTRAP_SERVERS=server1:9092,server2:9092
    
    # For demo purposes, we'll show both cases
    print("Environment variable PUBSUBLITE_USE_KAFKA not set:")
    client = UniversalPublisherClient()
    print(f"Backend type: {client.backend_type}")  # Should be "pubsublite"
    
    # Simulate environment variable being set
    os.environ['PUBSUBLITE_USE_KAFKA'] = 'true'
    
    print("\nEnvironment variable PUBSUBLITE_USE_KAFKA=true:")
    try:
        # This would require kafka_config to be provided
        kafka_config = create_kafka_config(
            bootstrap_servers=["localhost:9092"],
        )
        client = UniversalPublisherClient(kafka_config=kafka_config)
        print(f"Backend type: {client.backend_type}")  # Should be "kafka"
    except ImportError:
        print("Kafka support not available (confluent-kafka not installed)")
    finally:
        # Clean up
        del os.environ['PUBSUBLITE_USE_KAFKA']


def example_configuration_helpers():
    """Example showing configuration helpers."""
    print("\n=== Configuration Helpers Example ===")
    
    from google.cloud.pubsublite.cloudpubsub.config import (
        get_local_kafka_config,
        get_gcp_kafka_config,
        create_kafka_producer_config,
    )
    
    # Local development configuration
    try:
        local_config = get_local_kafka_config()
        print(f"Local Kafka config: {local_config.bootstrap_servers}")
    except ImportError:
        print("Kafka support not available for local config")
    
    # Production configuration  
    try:
        prod_config = get_gcp_kafka_config([
            "prod-server-1:9092",
            "prod-server-2:9092",
        ])
        print(f"Production Kafka config: {prod_config.bootstrap_servers}")
    except ImportError:
        print("Kafka support not available for production config")
    
    # Custom producer settings
    producer_config = create_kafka_producer_config()
    print(f"Default producer batch size: {producer_config['batch.size']}")


if __name__ == "__main__":
    print("Universal Publisher Client Examples")
    print("=" * 50)
    
    # Run examples (these will work with mock/test setup)
    example_pubsublite_backend()
    
    try:
        example_kafka_backend()
    except ImportError:
        print("\n=== Kafka Backend Example ===")
        print("Kafka support not available (confluent-kafka not installed)")
        print("Install with: pip install google-cloud-pubsublite[kafka]")
    
    example_environment_variable()
    example_configuration_helpers()
    
    print("\n" + "=" * 50)
    print("Examples completed!")
    print("\nNote: These examples use placeholder project/topic names.")
    print("Replace with your actual Google Cloud project and topic names.")
    print("For Kafka examples, replace with your actual cluster bootstrap servers.")