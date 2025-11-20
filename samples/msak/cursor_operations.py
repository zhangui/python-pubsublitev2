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
Cursor operations for Managed Service for Apache Kafka (MSAK) using CursorServiceClient.

This sample demonstrates cursor operations on Google Cloud's Managed Service for Apache Kafka
using the Pub/Sub Lite v1 CursorServiceClient with Kafka transport and Application
Default Credentials.
"""

import logging
import time
from google.cloud import pubsublite_v1
from google.cloud.pubsublite_v1.types import cursor, common

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def create_cursor_client(
    bootstrap_servers: str = "localhost:9092",
) -> pubsublite_v1.CursorServiceClient:
    """Create CursorServiceClient with Kafka transport.

    Args:
        bootstrap_servers: Kafka bootstrap servers

    Returns:
        CursorServiceClient configured for Kafka
    """
    # Create kafka config
    kafka_config = {
        'bootstrap.servers': bootstrap_servers,
        # Add SASL/SSL config if needed, but for this sample we might assume local or simple auth
        # For MSAK, we usually need SASL_SSL. 
        # But since admin_operations.py used 'cluster_id' (Control Plane), 
        # and we are using Data Plane, we need the actual bootstrap address.
        # We'll accept it as an arg.
    }

    # Create client with kafka transport
    print(f"Connecting to Kafka cluster: {bootstrap_servers}")
    
    client = pubsublite_v1.CursorServiceClient(
        transport="kafka",
        kafka_config=kafka_config,
    )

    print("CursorServiceClient created successfully with Kafka transport")
    return client


def commit_cursor(
    client: pubsublite_v1.CursorServiceClient,
    project_id: str,
    location: str,
    subscription_id: str,
    partition: int,
    offset: int,
):
    """Commit a cursor (offset).

    Args:
        client: CursorServiceClient
        project_id: Google Cloud project ID
        location: Cloud location
        subscription_id: Subscription ID (Consumer Group ID)
        partition: Partition number
        offset: Offset to commit
    """
    subscription_path = f"projects/{project_id}/locations/{location}/subscriptions/{subscription_id}"
    
    print(f"\nCommitting cursor for '{subscription_id}' partition {partition} to offset {offset}...")

    client.commit_cursor(
        request=cursor.CommitCursorRequest(
            subscription=subscription_path,
            partition=partition,
            cursor=common.Cursor(offset=offset)
        )
    )

    print(f"Cursor committed.")


def list_partition_cursors(
    client: pubsublite_v1.CursorServiceClient,
    project_id: str,
    location: str,
    subscription_id: str,
):
    """List cursors for a subscription.

    Args:
        client: CursorServiceClient
        project_id: Google Cloud project ID
        location: Cloud location
        subscription_id: Subscription ID (Consumer Group ID)
    """
    subscription_path = f"projects/{project_id}/locations/{location}/subscriptions/{subscription_id}"
    
    print(f"\nListing cursors for '{subscription_id}'...")

    response = client.list_partition_cursors(
        parent=subscription_path
    )

    for pc in response.partition_cursors:
        print(f"Partition {pc.partition}: Offset {pc.cursor.offset}")


def demo_cursor_operations(
    bootstrap_servers: str = "localhost:9092",
    project_id: str = "ygnahz-eg-codelab",
    location: str = "us-central1",
    subscription_id: str = "test-group",
    partition: int = 0,
):
    """Demonstrate cursor operations.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        project_id: Google Cloud project ID
        location: Cloud location
        subscription_id: Subscription ID (Consumer Group ID)
        partition: Partition to commit to
    """
    print(f"Creating CursorServiceClient for Kafka...")
    client = create_cursor_client(bootstrap_servers)

    try:
        # Commit a cursor
        # Note: This requires the consumer group to exist and be associated with a topic 
        # (which implies it must have committed before or we need to know the topic).
        # Our implementation tries to look up the topic from existing offsets.
        # If this is a fresh group, it might fail if we can't determine the topic.
        # But for a demo, we assume it might work if the group exists.
        
        offset = 100
        commit_cursor(client, project_id, location, subscription_id, partition, offset)

        # Wait for propagation
        time.sleep(1)

        # List cursors
        list_partition_cursors(client, project_id, location, subscription_id)
        
        # Commit a higher offset
        offset = 200
        commit_cursor(client, project_id, location, subscription_id, partition, offset)
        
        time.sleep(1)
        
        # List again
        list_partition_cursors(client, project_id, location, subscription_id)

        print("\nCursor operations completed successfully!")

    except Exception as e:
        print(f"\nError during cursor operations: {e}")
        # import traceback
        # traceback.print_exc()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Perform cursor operations on MSAK using Kafka transport"
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
        "--subscription-id",
        default="testtopic",
        help="Subscription ID (Consumer Group ID)"
    )
    parser.add_argument(
        "--partition",
        type=int,
        default=2,
        help="Partition number"
    )

    args = parser.parse_args()

    demo_cursor_operations(
        bootstrap_servers=args.bootstrap_servers,
        project_id=args.project_id,
        location=args.location,
        subscription_id=args.subscription_id,
        partition=args.partition,
    )
