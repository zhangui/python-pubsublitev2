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
Test AdminServiceClient with Kafka transport.

This test uses the low-level AdminServiceClient to test the Kafka transport directly.
"""

import logging
from google.cloud import pubsublite_v1
from google.cloud.pubsublite_v1.types import common

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
    from tokenprovider import TokenProvider
except ImportError:
    TokenProvider = None


def test_admin_service_kafka(
    bootstrap_servers: str,
    project_id: str,
    location: str,
):
    """Test AdminServiceClient with Kafka transport.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        project_id: Google Cloud project ID
        location: Cloud location
    """
    if TokenProvider is None:
        raise ImportError("TokenProvider not available")

    print("\n=== Testing AdminServiceClient with Kafka Transport ===\n")

    # Create Kafka admin config with OAuth authentication
    token_provider = TokenProvider()
    admin_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
        'socket.timeout.ms': 60000,
        'request.timeout.ms': 30000,
        'api.version.request.timeout.ms': 10000,
    }

    print(f"1. Creating AdminServiceClient with Kafka transport")
    print(f"   Bootstrap servers: {bootstrap_servers}")
    print(f"   Project: {project_id}")
    print(f"   Location: {location}")
    print(f"   Config keys: {list(admin_config.keys())}")

    try:
        # Create AdminServiceClient with Kafka transport
        client = pubsublite_v1.AdminServiceClient(
            transport="kafka",
            admin_config=admin_config,
        )
        print("   ✓ AdminServiceClient created successfully")
        print(f"   Transport type: {client.transport.kind}\n")
    except Exception as e:
        print(f"   ✗ Failed to create AdminServiceClient: {e}")
        import traceback
        traceback.print_exc()
        return

    # Test 1: List topics
    print("2. Testing list_topics()")
    parent = f"projects/{project_id}/locations/{location}"
    print(f"   Parent: {parent}")
    try:
        response = client.list_topics(parent=parent)
        topics = list(response)
        print(f"   ✓ Successfully listed topics")
        print(f"   Found {len(topics)} topics:")
        for topic in topics:
            print(f"     - {topic.name}")
            print(f"       Partitions: {topic.partition_config.count}")
    except Exception as e:
        print(f"   ✗ Failed to list topics: {e}")
        print(f"   Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return

    # Test 2: Create a topic
    test_topic_name = "test-service-topic"
    print(f"\n3. Testing create_topic()")
    print(f"   Topic name: {test_topic_name}")
    try:
        new_topic = common.Topic(
            partition_config=common.Topic.PartitionConfig(count=3)
        )

        created_topic = client.create_topic(
            parent=parent,
            topic=new_topic,
            topic_id=test_topic_name,
        )
        print(f"   ✓ Topic created successfully")
        print(f"     Name: {created_topic.name}")
        print(f"     Partitions: {created_topic.partition_config.count}")
    except Exception as e:
        print(f"   ✗ Failed to create topic: {e}")
        print(f"   Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        # Don't return, try to continue

    # Test 3: Get topic
    print(f"\n4. Testing get_topic()")
    topic_path = f"{parent}/topics/{test_topic_name}"
    print(f"   Topic path: {topic_path}")
    try:
        topic = client.get_topic(name=topic_path)
        print(f"   ✓ Topic retrieved successfully")
        print(f"     Name: {topic.name}")
        print(f"     Partitions: {topic.partition_config.count}")
    except Exception as e:
        print(f"   ✗ Failed to get topic: {e}")
        print(f"   Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return

    # Test 4: Get partition count
    print(f"\n5. Testing get_topic_partitions()")
    try:
        partitions = client.get_topic_partitions(name=topic_path)
        print(f"   ✓ Partition count retrieved successfully")
        print(f"     Partitions: {partitions.partition_count}")
    except Exception as e:
        print(f"   ✗ Failed to get partition count: {e}")
        print(f"   Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()

    # Test 5: Delete topic
    print(f"\n6. Testing delete_topic()")
    try:
        client.delete_topic(name=topic_path)
        print(f"   ✓ Topic deleted successfully")
    except Exception as e:
        print(f"   ✗ Failed to delete topic: {e}")
        print(f"   Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()

    # Final verification: List topics again
    print(f"\n7. Final verification - list_topics()")
    try:
        response = client.list_topics(parent=parent)
        topics = list(response)
        print(f"   ✓ Successfully listed topics")
        print(f"   Found {len(topics)} topics:")
        for topic in topics:
            print(f"     - {topic.name.split('/')[-1]}")
    except Exception as e:
        print(f"   ✗ Failed to list topics: {e}")
        print(f"   Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()

    print("\n=== AdminServiceClient Test Complete ===\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Test AdminServiceClient with Kafka transport"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--project-id",
        default="ygnahz-eg-codelab",
        help="Google Cloud project ID"
    )
    parser.add_argument(
        "--location",
        default="us-central1",
        help="Cloud location"
    )

    args = parser.parse_args()

    test_admin_service_kafka(
        bootstrap_servers=args.bootstrap_servers,
        project_id=args.project_id,
        location=args.location,
    )
