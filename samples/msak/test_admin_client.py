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
Test AdminClient with Kafka transport.

This test uses the high-level AdminClient (google.cloud.pubsublite.AdminClient)
instead of the low-level AdminServiceClient to help isolate issues.
"""

import logging
from google.cloud.pubsublite import AdminClient
from google.cloud.pubsublite.types import CloudRegion, LocationPath, TopicPath
from google.cloud.pubsublite_v1 import Topic

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
    from tokenprovider import TokenProvider
except ImportError:
    TokenProvider = None


def test_admin_client_kafka(
    bootstrap_servers: str,
    project_id: str,
    location: str,
):
    """Test AdminClient with Kafka transport.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        project_id: Google Cloud project ID
        location: Cloud location
    """
    if TokenProvider is None:
        raise ImportError("TokenProvider not available")

    print("\n=== Testing AdminClient with Kafka Transport ===\n")

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

    print(f"1. Creating AdminClient with Kafka transport")
    print(f"   Bootstrap servers: {bootstrap_servers}")
    print(f"   Project: {project_id}")
    print(f"   Location: {location}")

    try:
        # Create AdminClient with Kafka transport
        region = CloudRegion(location)
        client = AdminClient(
            region=region,
            transport="kafka",
            admin_config=admin_config,
        )
        print("   ✓ AdminClient created successfully\n")
    except Exception as e:
        print(f"   ✗ Failed to create AdminClient: {e}")
        import traceback
        traceback.print_exc()
        return

    # Test 1: List topics
    print("2. Testing list_topics()")
    try:
        location_path = LocationPath(project_id, location)
        topics = client.list_topics(location_path)
        print(f"   ✓ Successfully listed topics")
        print(f"   Found {len(topics)} topics:")
        for topic in topics:
            print(f"     - {topic.name}")
            print(f"       Partitions: {topic.partition_config.count}")
    except Exception as e:
        print(f"   ✗ Failed to list topics: {e}")
        import traceback
        traceback.print_exc()
        return

    # Test 2: Create a topic
    test_topic_name = "testtopic"
    print(f"\n3. Testing create_topic()")
    print(f"   Topic name: {test_topic_name}")
    try:
        topic_path = TopicPath(project_id, location, test_topic_name)

        new_topic = Topic(
            name=str(topic_path),
            partition_config=Topic.PartitionConfig(count=3),
        )

        created_topic = client.create_topic(new_topic)
        print(f"   ✓ Topic created successfully")
        print(f"     Name: {created_topic.name}")
        print(f"     Partitions: {created_topic.partition_config.count}")
    except Exception as e:
        print(f"   ✗ Failed to create topic: {e}")
        import traceback
        traceback.print_exc()
        # Don't return, try to continue with get_topic if it already exists

    # Test 3: Get topic
    print(f"\n4. Testing get_topic()")
    try:
        topic_path = TopicPath(project_id, location, test_topic_name)
        topic = client.get_topic(topic_path)
        print(f"   ✓ Topic retrieved successfully")
        print(f"     Name: {topic.name}")
        print(f"     Partitions: {topic.partition_config.count}")
    except Exception as e:
        print(f"   ✗ Failed to get topic: {e}")
        import traceback
        traceback.print_exc()
        return

    # Test 4: Get partition count
    print(f"\n5. Testing get_topic_partition_count()")
    try:
        topic_path = TopicPath(project_id, location, test_topic_name)
        partition_count = client.get_topic_partition_count(topic_path)
        print(f"   ✓ Partition count retrieved successfully")
        print(f"     Partitions: {partition_count}")
    except Exception as e:
        print(f"   ✗ Failed to get partition count: {e}")
        import traceback
        traceback.print_exc()

    # Test 5: Delete topic
    print(f"\n6. Testing delete_topic()")
    try:
        topic_path = TopicPath(project_id, location, test_topic_name)
        client.delete_topic(topic_path)
        print(f"   ✓ Topic deleted successfully")
    except Exception as e:
        print(f"   ✗ Failed to delete topic: {e}")
        import traceback
        traceback.print_exc()

    # Final verification: List topics again
    print(f"\n7. Final verification - list_topics()")
    try:
        location_path = LocationPath(project_id, location)
        topics = client.list_topics(location_path)
        print(f"   ✓ Successfully listed topics")
        print(f"   Found {len(topics)} topics:")
        for topic in topics:
            print(f"     - {topic.name.split('/')[-1]}")
    except Exception as e:
        print(f"   ✗ Failed to list topics: {e}")
        import traceback
        traceback.print_exc()

    print("\n=== AdminClient Test Complete ===\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Test AdminClient with Kafka transport"
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

    test_admin_client_kafka(
        bootstrap_servers=args.bootstrap_servers,
        project_id=args.project_id,
        location=args.location,
    )
