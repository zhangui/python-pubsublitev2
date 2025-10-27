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
Admin operations for Managed Service for Apache Kafka (MSAK) using AdminServiceClient.

This sample demonstrates admin operations on Google Cloud's Managed Service for Apache Kafka
using the Pub/Sub Lite v1 AdminServiceClient with Kafka transport and TokenProvider
for OAuth authentication.
"""

import logging
from typing import Optional
from google.cloud import pubsublite_v1

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
    from tokenprovider import TokenProvider
except ImportError:
    TokenProvider = None


def create_admin_client(
    bootstrap_servers: str,
    project_id: str,
    location: str,
) -> pubsublite_v1.AdminServiceClient:
    """Create AdminServiceClient with Kafka transport.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        project_id: Google Cloud project ID
        location: Cloud location

    Returns:
        AdminServiceClient configured for Kafka
    """
    if TokenProvider is None:
        raise ImportError("TokenProvider not available")

    # Create Kafka admin config with OAuth authentication
    token_provider = TokenProvider()
    admin_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
    }

    # Create client with Kafka transport
    return pubsublite_v1.AdminServiceClient(
        transport="kafka",
        admin_config=admin_config,
    )


def create_topic(
    client: pubsublite_v1.AdminServiceClient,
    project_id: str,
    location: str,
    topic_id: str,
    num_partitions: int = 3,
):
    """Create a new Kafka topic.

    Args:
        client: AdminServiceClient
        project_id: Google Cloud project ID
        location: Cloud location
        topic_id: Topic name
        num_partitions: Number of partitions
    """
    parent = f"projects/{project_id}/locations/{location}"

    print(f"\nCreating topic '{topic_id}' with {num_partitions} partitions...")

    from google.cloud.pubsublite_v1.types import common

    topic = client.create_topic(
        parent=parent,
        topic=common.Topic(
            partition_config=common.Topic.PartitionConfig(
                count=num_partitions
            )
        ),
        topic_id=topic_id,
    )

    print(f"Topic created: {topic.name}")
    print(f"  Partitions: {topic.partition_config.count}")


def get_topic(
    client: pubsublite_v1.AdminServiceClient,
    project_id: str,
    location: str,
    topic_id: str,
):
    """Get topic details.

    Args:
        client: AdminServiceClient
        project_id: Google Cloud project ID
        location: Cloud location
        topic_id: Topic name
    """
    topic_path = f"projects/{project_id}/locations/{location}/topics/{topic_id}"

    print(f"\nGetting topic '{topic_id}'...")

    topic = client.get_topic(name=topic_path)

    print(f"Topic: {topic.name}")
    print(f"  Partitions: {topic.partition_config.count}")


def list_topics(
    client: pubsublite_v1.AdminServiceClient,
    project_id: str,
    location: str,
):
    """List all topics.

    Args:
        client: AdminServiceClient
        project_id: Google Cloud project ID
        location: Cloud location
    """
    parent = f"projects/{project_id}/locations/{location}"

    print(f"\nListing topics in {location}...")

    topics = client.list_topics(parent=parent)

    for i, topic in enumerate(topics, 1):
        print(f"{i}. {topic.name}")
        print(f"   Partitions: {topic.partition_config.count}")


def delete_topic(
    client: pubsublite_v1.AdminServiceClient,
    project_id: str,
    location: str,
    topic_id: str,
):
    """Delete a topic.

    Args:
        client: AdminServiceClient
        project_id: Google Cloud project ID
        location: Cloud location
        topic_id: Topic name
    """
    topic_path = f"projects/{project_id}/locations/{location}/topics/{topic_id}"

    print(f"\nDeleting topic '{topic_id}'...")

    client.delete_topic(name=topic_path)

    print(f"Topic deleted: {topic_path}")


def demo_admin_operations(
    bootstrap_servers: str,
    project_id: str,
    location: str,
    topic_id: str = "test-admin-topic",
):
    """Demonstrate admin operations.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        project_id: Google Cloud project ID
        location: Cloud location
        topic_id: Topic name to create/manipulate
    """
    print(f"Creating AdminServiceClient for Kafka...")
    client = create_admin_client(bootstrap_servers, project_id, location)

    try:
        # List existing topics
        list_topics(client, project_id, location)

        # Create a new topic
        create_topic(client, project_id, location, topic_id, num_partitions=3)

        # Get the topic details
        get_topic(client, project_id, location, topic_id)

        # List topics again to see the new topic
        list_topics(client, project_id, location)

        # Delete the topic
        delete_topic(client, project_id, location, topic_id)

        # List topics again to confirm deletion
        list_topics(client, project_id, location)

        print("\nAdmin operations completed successfully!")

    except Exception as e:
        print(f"\nError during admin operations: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Perform admin operations on MSAK using Kafka transport"
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
    parser.add_argument(
        "--topic-id",
        default="test-admin-topic",
        help="Topic name for demo operations"
    )

    args = parser.parse_args()

    demo_admin_operations(
        bootstrap_servers=args.bootstrap_servers,
        project_id=args.project_id,
        location=args.location,
        topic_id=args.topic_id,
    )
