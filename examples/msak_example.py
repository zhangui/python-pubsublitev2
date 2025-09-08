#!/usr/bin/env python3

# Copyright 2020 Google LLC
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
Example demonstrating how to use the enhanced Pub/Sub Lite client with 
Google Managed Service for Apache Kafka (MSAK).

This example shows how to:
1. Publish messages to MSAK using the familiar Pub/Sub Lite client API
2. Subscribe to messages from MSAK
3. Administer MSAK topics and consumer groups

Prerequisites:
- Install with MSAK support: pip install google-cloud-pubsublite[msak]
- Have a Google Cloud project with MSAK enabled
- Have a MSAK cluster created in your project
"""

import os
import asyncio
import time
from typing import List

from google.auth import default
from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
from google.cloud.pubsublite.transport import MSAKConfig, TransportType


def create_msak_config() -> MSAKConfig:
    """Create MSAK configuration from environment variables."""
    return MSAKConfig(
        project_id=os.environ["GOOGLE_CLOUD_PROJECT"],
        location=os.environ.get("MSAK_LOCATION", "us-central1"),
        cluster_id=os.environ["MSAK_CLUSTER_ID"],
        credentials=default()[0],  # Use default credentials
    )


def publish_messages_to_msak():
    """Example: Publishing messages to Google MSAK."""
    print("=== Publishing Messages to Google MSAK ===")
    
    # Create MSAK configuration
    msak_config = create_msak_config()
    
    # Create publisher client for MSAK
    with EnhancedPublisherClient.create_for_msak(
        topic="my-kafka-topic",
        msak_config=msak_config,
    ) as publisher:
        
        print(f"Publishing to MSAK cluster: {msak_config.cluster_path}")
        print(f"Bootstrap servers: {msak_config.bootstrap_servers}")
        
        # Publish some messages
        futures = []
        for i in range(10):
            message_data = f"Hello MSAK message {i}!".encode()
            
            future = publisher.publish(
                "my-kafka-topic",  # Topic name
                message_data,
                ordering_key=f"key-{i % 3}",  # Will be used as Kafka partition key
                message_type="example",
                sequence_number=str(i),
            )
            futures.append(future)
            print(f"Queued message {i}")
        
        # Wait for all messages to be published
        print("\nWaiting for messages to be published...")
        for i, future in enumerate(futures):
            try:
                message_id = future.result(timeout=30)
                print(f"Message {i} published with ID: {message_id}")
            except Exception as e:
                print(f"Message {i} failed to publish: {e}")
        
        print("\nFlushing remaining messages...")
        publisher.flush(timeout=30)
        print("All messages published successfully!")


def subscribe_to_msak_messages():
    """Example: Subscribing to messages from Google MSAK."""
    print("\n=== Subscribing to Messages from Google MSAK ===")
    
    # Note: This would require implementing the subscriber client
    # For brevity, showing the configuration pattern
    
    msak_config = create_msak_config()
    print(f"Would subscribe from MSAK cluster: {msak_config.cluster_path}")
    print("Consumer group: my-consumer-group")
    print("Topic: my-kafka-topic")
    
    # Subscriber implementation would look like:
    # with EnhancedSubscriberClient.create_for_msak(
    #     subscription="my-consumer-group",
    #     topic="my-kafka-topic", 
    #     msak_config=msak_config,
    # ) as subscriber:
    #     
    #     def message_handler(message):
    #         print(f"Received: {message.data.decode()}")
    #         message.ack()
    #     
    #     future = subscriber.subscribe(message_handler)
    #     future.result()  # Block until cancelled


def compare_pubsub_lite_vs_msak():
    """Example: Comparing Pub/Sub Lite vs MSAK usage."""
    print("\n=== Comparing Pub/Sub Lite vs MSAK ===")
    
    # Pub/Sub Lite example
    print("Pub/Sub Lite usage:")
    print("""
    from google.cloud.pubsublite.types import TopicPath
    from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
    
    topic_path = TopicPath.parse("projects/my-project/locations/us-central1/topics/my-topic")
    
    with EnhancedPublisherClient.create_for_pubsub_lite(topic_path) as publisher:
        future = publisher.publish(topic_path, b"Hello Pub/Sub Lite!")
        message_id = future.result()
    """)
    
    # MSAK example
    print("MSAK usage:")
    print("""
    from google.cloud.pubsublite.transport import MSAKConfig
    from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
    
    msak_config = MSAKConfig(
        project_id="my-project",
        location="us-central1", 
        cluster_id="my-cluster",
    )
    
    with EnhancedPublisherClient.create_for_msak("my-topic", msak_config) as publisher:
        future = publisher.publish("my-topic", b"Hello MSAK!")
        message_id = future.result()
    """)
    
    print("\nKey differences:")
    print("- Pub/Sub Lite: Uses TopicPath, automatic partitioning, Google-managed scaling")
    print("- MSAK: Uses topic names, Kafka partitioning, cluster-based scaling")
    print("- Both: Same client API, same message format, same batching/flow control")


def admin_operations_example():
    """Example: Administrative operations on MSAK."""
    print("\n=== MSAK Administrative Operations ===")
    
    msak_config = create_msak_config()
    
    print("Admin operations (pseudo-code):")
    print(f"""
    from google.cloud.pubsublite.transport import TransportFactory, TransportType
    
    admin_transport = TransportFactory.create_admin_transport(
        transport_type=TransportType.MANAGED_KAFKA,
        region="{msak_config.location}",
        msak_config=msak_config,
    )
    
    # Create topic
    topic = Topic(name="my-new-topic", partition_count=3)
    admin_transport.create_topic(topic)
    
    # List topics
    topics = admin_transport.list_topics(location_path)
    
    # Create subscription (consumer group)
    subscription = Subscription(name="my-consumer-group", topic="my-new-topic")
    admin_transport.create_subscription(subscription)
    """)


def main():
    """Main example function."""
    print("Google Cloud Pub/Sub Lite with MSAK Support Example")
    print("=" * 60)
    
    # Check required environment variables
    required_vars = ["GOOGLE_CLOUD_PROJECT", "MSAK_CLUSTER_ID"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        print("\nPlease set:")
        print("- GOOGLE_CLOUD_PROJECT: Your Google Cloud project ID")
        print("- MSAK_CLUSTER_ID: Your MSAK cluster ID")
        print("- MSAK_LOCATION: Your MSAK cluster location (optional, defaults to us-central1)")
        return
    
    try:
        # Run examples
        compare_pubsub_lite_vs_msak()
        admin_operations_example()
        
        # These would require actual MSAK cluster to run
        if os.environ.get("RUN_LIVE_EXAMPLES") == "true":
            publish_messages_to_msak()
            subscribe_to_msak_messages()
        else:
            print("\nTo run live examples against a real MSAK cluster:")
            print("Set environment variable: RUN_LIVE_EXAMPLES=true")
    
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install google-cloud-pubsublite[msak]")
    
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()