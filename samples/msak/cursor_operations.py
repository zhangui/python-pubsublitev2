#!/usr/bin/env python

# Copyright 2025 Google Inc. All Rights Reserved.
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

"""Sample script to test CursorService operations with Kafka backend."""

import argparse
import sys
from google.cloud.pubsublite_v1.services.cursor_service import CursorServiceClient
from google.cloud.pubsublite_v1.types import cursor as cursor_types
from google.cloud.pubsublite_v1.types import common


def commit_cursor(
    project_id: str,
    location: str,
    subscription_id: str,
    partition: int,
    offset: int,
    bootstrap_servers: str,
) -> None:
    """Commit a cursor position for a subscription partition."""
    print(f"\n=== Committing Cursor ===")
    print(f"Subscription: {subscription_id}")
    print(f"Partition: {partition}")
    print(f"Offset: {offset}")

    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'cursor-service-test',
    }

    # Create CursorServiceClient with Kafka transport
    client = CursorServiceClient(
        transport="kafka",
        consumer_config=consumer_config
    )

    # Build subscription path
    subscription_path = f"projects/{project_id}/locations/{location}/subscriptions/{subscription_id}"

    # Create commit request
    request = cursor_types.CommitCursorRequest(
        subscription=subscription_path,
        partition=partition,
        cursor=common.Cursor(offset=offset)
    )

    try:
        # Commit the cursor
        response = client.commit_cursor(request=request)
        print(f"✓ Successfully committed cursor at offset {offset}")
    except Exception as e:
        print(f"✗ Error committing cursor: {e}")
        raise


def list_partition_cursors(
    project_id: str,
    location: str,
    subscription_id: str,
    bootstrap_servers: str,
) -> None:
    """List committed cursors for all partitions of a subscription."""
    print(f"\n=== Listing Partition Cursors ===")
    print(f"Subscription: {subscription_id}")

    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'cursor-service-test',
    }

    # Create CursorServiceClient with Kafka transport
    client = CursorServiceClient(
        transport="kafka",
        consumer_config=consumer_config
    )

    # Build subscription path
    subscription_path = f"projects/{project_id}/locations/{location}/subscriptions/{subscription_id}"

    # Create list request
    request = cursor_types.ListPartitionCursorsRequest(
        parent=subscription_path,
        page_size=10
    )

    try:
        # List partition cursors
        response = client.list_partition_cursors(request=request)

        print(f"\nPartition cursors:")
        for partition_cursor in response.partition_cursors:
            print(f"  Partition {partition_cursor.partition}: offset={partition_cursor.cursor.offset}")

        if response.next_page_token:
            print(f"Next page token: {response.next_page_token}")
        else:
            print("No more pages")

    except Exception as e:
        print(f"✗ Error listing partition cursors: {e}")
        raise


def streaming_commit_cursor(
    project_id: str,
    location: str,
    subscription_id: str,
    partition: int,
    start_offset: int,
    num_commits: int,
    bootstrap_servers: str,
) -> None:
    """Test streaming commit cursor with multiple commits."""
    print(f"\n=== Streaming Commit Cursor ===")
    print(f"Subscription: {subscription_id}")
    print(f"Partition: {partition}")
    print(f"Starting offset: {start_offset}")
    print(f"Number of commits: {num_commits}")

    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'cursor-service-test',
    }

    # Create CursorServiceClient with Kafka transport
    client = CursorServiceClient(
        transport="kafka",
        consumer_config=consumer_config
    )

    # Build subscription path
    subscription_path = f"projects/{project_id}/locations/{location}/subscriptions/{subscription_id}"

    def request_iterator():
        """Generate streaming requests."""
        # Send initial request
        yield cursor_types.StreamingCommitCursorRequest(
            initial=cursor_types.InitialCommitCursorRequest(
                subscription=subscription_path,
                partition=partition
            )
        )

        # Send multiple commit requests
        for i in range(num_commits):
            offset = start_offset + i
            yield cursor_types.StreamingCommitCursorRequest(
                commit=cursor_types.SequencedCommitCursorRequest(
                    cursor=common.Cursor(offset=offset)
                )
            )
            print(f"  Sent commit for offset {offset}")

    try:
        # Start streaming
        responses = client.streaming_commit_cursor(requests=request_iterator())

        # Process responses
        response_count = 0
        for response in responses:
            if response.initial:
                print("✓ Received initial response")
            elif response.commit:
                response_count += 1
                print(f"✓ Received commit response #{response_count}: "
                      f"acknowledged={response.commit.acknowledged_commits}")

        print(f"\n✓ Successfully completed {response_count} streaming commits")

    except Exception as e:
        print(f"✗ Error in streaming commit: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Test CursorService operations with Kafka backend"
    )
    parser.add_argument(
        "--project-id",
        default="ygnahz-eg-codelab",
        help="Google Cloud project ID"
    )
    parser.add_argument(
        "--location",
        default="us-central1",
        help="Location of the Kafka cluster"
    )
    parser.add_argument(
        "--subscription-id",
        default="test-subscription",
        help="Subscription ID (consumer group name)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--operation",
        choices=["commit", "list", "streaming", "all"],
        default="all",
        help="Which operation to test"
    )
    parser.add_argument(
        "--partition",
        type=int,
        default=0,
        help="Partition number for commit operations"
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=100,
        help="Offset to commit"
    )

    args = parser.parse_args()

    print("\nMake sure you've run: gcloud auth application-default login\n")

    print("=" * 60)
    print("CURSOR SERVICE KAFKA TRANSPORT TEST")
    print("=" * 60)
    print(f"Project: {args.project_id}")
    print(f"Location: {args.location}")
    print(f"Bootstrap Servers: {args.bootstrap_servers}")
    print(f"Subscription: {args.subscription_id}")

    try:
        if args.operation in ["commit", "all"]:
            commit_cursor(
                args.project_id,
                args.location,
                args.subscription_id,
                args.partition,
                args.offset,
                args.bootstrap_servers
            )

        if args.operation in ["list", "all"]:
            list_partition_cursors(
                args.project_id,
                args.location,
                args.subscription_id,
                args.bootstrap_servers
            )

        if args.operation in ["streaming", "all"]:
            streaming_commit_cursor(
                args.project_id,
                args.location,
                args.subscription_id,
                args.partition,
                args.offset,
                5,  # Number of commits
                args.bootstrap_servers
            )

        print("\n" + "=" * 60)
        print("✓ ALL TESTS COMPLETED SUCCESSFULLY")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ TESTS FAILED: {e}")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()