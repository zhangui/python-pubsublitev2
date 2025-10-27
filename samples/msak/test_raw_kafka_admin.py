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
Test raw confluent_kafka AdminClient connection.

This bypasses all our transport code to test if the basic Kafka AdminClient
can connect and authenticate.
"""

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
    from tokenprovider import TokenProvider
except ImportError:
    TokenProvider = None

try:
    from confluent_kafka.admin import AdminClient
except ImportError:
    AdminClient = None


def test_raw_kafka_admin(bootstrap_servers: str):
    """Test raw Kafka AdminClient connection.

    Args:
        bootstrap_servers: Kafka bootstrap servers
    """
    if TokenProvider is None:
        raise ImportError("TokenProvider not available")

    if AdminClient is None:
        raise ImportError("confluent_kafka not available")

    print("\n=== Testing Raw Kafka AdminClient ===\n")

    # Create Kafka admin config
    token_provider = TokenProvider()
    admin_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
        'socket.timeout.ms': 60000,
        'request.timeout.ms': 30000,
        'api.version.request.timeout.ms': 10000,
        'debug': 'broker,security',  # Enable debug logging
    }

    print(f"1. Creating Kafka AdminClient")
    print(f"   Bootstrap servers: {bootstrap_servers}")
    print(f"   Config:")
    for key, value in admin_config.items():
        if key != 'oauth_cb':
            print(f"     {key}: {value}")

    try:
        admin_client = AdminClient(admin_config)
        print("   ✓ AdminClient created\n")
    except Exception as e:
        print(f"   ✗ Failed to create AdminClient: {e}")
        import traceback
        traceback.print_exc()
        return

    # Test 1: List topics with longer timeout
    print("2. Testing list_topics() with 60 second timeout")
    try:
        print("   Calling list_topics()...")
        metadata = admin_client.list_topics(timeout=60)

        print(f"   ✓ Successfully retrieved metadata")
        print(f"   Cluster ID: {metadata.cluster_id}")
        print(f"   Controller ID: {metadata.controller_id}")
        print(f"   Total topics: {len(metadata.topics)}")

        print("\n   Topics found:")
        for topic_name, topic_metadata in metadata.topics.items():
            if not topic_name.startswith('_'):  # Skip internal topics
                print(f"     - {topic_name}")
                print(f"       Partitions: {len(topic_metadata.partitions)}")
                print(f"       Error: {topic_metadata.error}")

    except Exception as e:
        print(f"   ✗ Failed to list topics: {e}")
        print(f"   Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return

    print("\n=== Raw Kafka AdminClient Test Complete ===\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Test raw Kafka AdminClient connection"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092",
        help="Kafka bootstrap servers"
    )

    args = parser.parse_args()

    test_raw_kafka_admin(
        bootstrap_servers=args.bootstrap_servers,
    )
