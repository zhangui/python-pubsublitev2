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

try:
    from tokenprovider import TokenProvider
except ImportError:
    TokenProvider = None


def commit_cursor(
    project_id: str,
    location: str,
    subscription_id: str,
    partition: int,
    offset: int,
    bootstrap_servers: str,
    topic: str = None,
) -> None:
    """Commit a cursor position for a subscription partition."""
    if TokenProvider is None:
        raise ImportError("TokenProvider not available. Ensure tokenprovider.py is in the same directory.")

    print(f"\n=== Committing Cursor ===")
    print(f"Subscription: {subscription_id}")
    print(f"Topic: {topic}")
    print(f"Partition: {partition}")
    print(f"Offset: {offset}")

    # Configure Kafka consumer with OAuth authentication
    print("[TEST] Creating TokenProvider...")
    try:
        token_provider = TokenProvider()
        print("[TEST] TokenProvider created successfully")
    except Exception as e:
        print(f"[TEST ERROR] Failed to create TokenProvider: {e}")
        import traceback
        traceback.print_exc()
        raise

    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
        'group.id': subscription_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'default_topic': topic,
    }
    print(f"[TEST] Consumer config created: bootstrap={bootstrap_servers}")

    # Create CursorServiceClient with Kafka transport
    print("[TEST] Creating CursorServiceClient with Kafka transport...")
    try:
        client = CursorServiceClient(
            transport="kafka",
            consumer_config=consumer_config
        )
        print("[TEST] Client created successfully")
        print(f"[TEST] Client transport type: {type(client._transport).__name__}")
    except Exception as e:
        print(f"[TEST ERROR] Failed to create client: {e}")
        import traceback
        traceback.print_exc()
        raise

    # Build subscription path
    subscription_path = f"projects/{project_id}/locations/{location}/subscriptions/{subscription_id}"
    print(f"[TEST] Subscription path: {subscription_path}")

    # Create commit request
    print(f"[TEST] Creating CommitCursorRequest...")
    request = cursor_types.CommitCursorRequest(
        subscription=subscription_path,
        partition=partition,
        cursor=common.Cursor(offset=offset)
    )
    print(f"[TEST] Request created: partition={request.partition}, offset={request.cursor.offset}")

    try:
        # Commit the cursor
        print(f"[TEST] Calling client.commit_cursor()...")
        response = client.commit_cursor(request=request)
        print(f"[TEST] commit_cursor() returned: {response}")
        print(f"✓ Successfully committed cursor at offset {offset}")
    except Exception as e:
        print(f"[TEST ERROR] Exception during commit_cursor: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        print(f"✗ Error committing cursor: {e}")
        raise


def list_partition_cursors(
    project_id: str,
    location: str,
    subscription_id: str,
    bootstrap_servers: str,
    topic: str = None,
) -> None:
    """List committed cursors for all partitions of a subscription."""
    if TokenProvider is None:
        raise ImportError("TokenProvider not available. Ensure tokenprovider.py is in the same directory.")

    print(f"\n=== Listing Partition Cursors ===")
    print(f"Subscription: {subscription_id}")
    print(f"Topic: {topic}")

    # Configure Kafka consumer with OAuth authentication
    token_provider = TokenProvider()
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
        'group.id': subscription_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'default_topic': topic,
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
    topic: str = None,
) -> None:
    """Test streaming commit cursor with multiple commits."""
    if TokenProvider is None:
        raise ImportError("TokenProvider not available. Ensure tokenprovider.py is in the same directory.")

    print(f"\n=== Streaming Commit Cursor ===")
    print(f"Subscription: {subscription_id}")
    print(f"Topic: {topic}")
    print(f"Partition: {partition}")
    print(f"Starting offset: {start_offset}")
    print(f"Number of commits: {num_commits}")

    # Configure Kafka consumer with OAuth authentication
    token_provider = TokenProvider()
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
        'group.id': subscription_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'default_topic': topic,
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


def verify_cursor_commit(
    project_id: str,
    location: str,
    subscription_id: str,
    partition: int,
    offset: int,
    bootstrap_servers: str,
    topic: str = None,
) -> None:
    """Verify that committing a cursor affects subsequent reads."""
    if TokenProvider is None:
        raise ImportError("TokenProvider not available. Ensure tokenprovider.py is in the same directory.")

    print(f"\n=== Verifying Cursor Commit ===")
    print(f"Step 1: Commit cursor at offset {offset}")

    # First commit the cursor
    commit_cursor(
        project_id,
        location,
        subscription_id,
        partition,
        offset,
        bootstrap_servers,
        topic
    )

    print(f"\nStep 2: Read messages to verify they start from offset {offset}")

    # Now create a consumer and verify it starts reading from the committed offset
    try:
        from confluent_kafka import Consumer, TopicPartition

        token_provider = TokenProvider()
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': token_provider.get_token,
            'group.id': subscription_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }

        print("[VERIFY] Creating Kafka consumer...")
        consumer = Consumer(consumer_config)

        # Subscribe to the topic
        print(f"[VERIFY] Subscribing to topic {topic}...")
        consumer.subscribe([topic])

        # Poll once to join the consumer group
        print("[VERIFY] Polling to join consumer group...")
        consumer.poll(timeout=2.0)

        # Check the committed offset
        tp = TopicPartition(topic, partition)
        committed = consumer.committed([tp], timeout=10.0)

        if committed and len(committed) > 0:
            committed_offset = committed[0].offset
            print(f"[VERIFY] Committed offset for partition {partition}: {committed_offset}")

            # In Kafka, committed offset is the NEXT offset to read
            # So if we committed offset N, the committed value should be N+1
            expected_committed = offset + 1
            if committed_offset == expected_committed:
                print(f"✓ Verification successful: Committed offset is {committed_offset} (next to read after {offset})")
            else:
                print(f"✗ Verification failed: Expected committed offset {expected_committed}, got {committed_offset}")
        else:
            print(f"✗ No committed offset found for partition {partition}")

        # Now assign the partition and verify the position
        print(f"[VERIFY] Assigning partition {partition}...")
        consumer.assign([tp])

        # Check the current position (where we'll read from next)
        position = consumer.position([tp])
        if position and len(position) > 0:
            current_position = position[0].offset
            print(f"[VERIFY] Current read position: {current_position}")

            if current_position == offset + 1:
                print(f"✓ Position verified: Consumer will read from offset {current_position}")
            else:
                print(f"⚠ Position is {current_position}, expected {offset + 1}")

        # Try to read a message
        print(f"[VERIFY] Attempting to read messages starting from offset {offset + 1}...")
        msg = consumer.poll(timeout=5.0)

        if msg is None:
            print("[VERIFY] No messages available (topic may be empty or at end)")
        elif msg.error():
            print(f"[VERIFY] Error polling: {msg.error()}")
        else:
            print(f"✓ Read message: partition={msg.partition()}, offset={msg.offset()}, key={msg.key()}")
            if msg.offset() >= offset + 1:
                print(f"✓ Message offset {msg.offset()} is >= committed position {offset + 1}")
            else:
                print(f"⚠ Message offset {msg.offset()} is < committed position {offset + 1}")

        consumer.close()
        print("[VERIFY] Consumer closed")

    except ImportError:
        print("✗ confluent-kafka not available, skipping verification")
    except Exception as e:
        print(f"✗ Error during verification: {e}")
        import traceback
        traceback.print_exc()
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
        default="bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.google:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--operation",
        choices=["commit", "list", "streaming", "verify", "all"],
        default="all",
        help="Which operation to test"
    )
    parser.add_argument(
        "--partition",
        type=int,
        default=2,
        help="Partition number for commit operations"
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=1,
        help="Offset to commit"
    )
    parser.add_argument(
        "--topic",
        default="testtopic",
        help="Kafka topic name"
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
    print(f"Topic: {args.topic}")
    print(f"Operation: {args.operation}")

    # Check imports
    print("\n[MAIN] Checking imports...")
    print(f"[MAIN] CursorServiceClient: {CursorServiceClient}")
    print(f"[MAIN] TokenProvider: {TokenProvider}")
    print(f"[MAIN] cursor_types: {cursor_types}")
    print(f"[MAIN] common: {common}")

    # Check if Kafka transport is registered
    print("\n[MAIN] Checking transport registry...")
    try:
        transport_class = CursorServiceClient.get_transport_class("kafka")
        print(f"[MAIN] Kafka transport class: {transport_class}")
    except KeyError as e:
        print(f"[MAIN ERROR] Kafka transport not registered: {e}")
        print(f"[MAIN] Available transports: {list(CursorServiceClient._transport_registry.keys())}")
    except Exception as e:
        print(f"[MAIN ERROR] Error checking transport: {e}")

    try:
        if args.operation in ["commit", "all"]:
            commit_cursor(
                args.project_id,
                args.location,
                args.subscription_id,
                args.partition,
                args.offset,
                args.bootstrap_servers,
                args.topic
            )

        if args.operation in ["list", "all"]:
            list_partition_cursors(
                args.project_id,
                args.location,
                args.subscription_id,
                args.bootstrap_servers,
                args.topic
            )

        if args.operation in ["streaming", "all"]:
            streaming_commit_cursor(
                args.project_id,
                args.location,
                args.subscription_id,
                args.partition,
                args.offset,
                5,  # Number of commits
                args.bootstrap_servers,
                args.topic
            )

        if args.operation == "verify":
            verify_cursor_commit(
                args.project_id,
                args.location,
                args.subscription_id,
                args.partition,
                args.offset,
                args.bootstrap_servers,
                args.topic
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