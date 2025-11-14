# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
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
#
"""Kafka transport for CursorService."""

from typing import Callable, Dict, Optional, Sequence, Union, Iterator, List, Tuple
import threading
import queue
import time
from collections import defaultdict
import warnings

from google.api_core import exceptions as core_exceptions
from google.api_core import gapic_v1
from google.auth import credentials as ga_credentials
from google.cloud.pubsublite_v1.services.cursor_service.transports.base import (
    CursorServiceTransport,
    DEFAULT_CLIENT_INFO,
)
from google.cloud.pubsublite_v1.types import cursor
from google.cloud.pubsublite_v1.types import common

try:
    from confluent_kafka import Consumer, TopicPartition, KafkaException
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False


class StreamingCommitHandler:
    """Handles streaming commit cursor requests with batching."""

    def __init__(self, consumer: "Consumer", topic: str):
        """Initialize the streaming commit handler.

        Args:
            consumer: Kafka consumer instance
            topic: Topic name for commits
        """
        self.consumer = consumer
        self.topic = topic
        self.commit_queue = queue.Queue()
        self.response_queue = queue.Queue()
        self.is_running = False
        self.worker_thread = None
        self.pending_commits = defaultdict(list)
        self.acknowledged_count = 0

    def start(self):
        """Start the background commit worker."""
        if self.is_running:
            return

        self.is_running = True
        self.worker_thread = threading.Thread(target=self._commit_worker)
        self.worker_thread.daemon = True
        self.worker_thread.start()

    def stop(self):
        """Stop the background commit worker."""
        self.is_running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5)

    def add_commit(self, request: cursor.SequencedCommitCursorRequest):
        """Add a commit request to the queue."""
        self.commit_queue.put(request)

    def get_response(self) -> cursor.SequencedCommitCursorResponse:
        """Get the next response from the queue."""
        return self.response_queue.get()

    def _commit_worker(self):
        """Background worker that processes commit requests."""
        batch_timeout = 0.1  # 100ms batch window
        last_commit_time = time.time()

        while self.is_running:
            try:
                # Collect commits for batching
                deadline = last_commit_time + batch_timeout
                commits_to_process = []

                while time.time() < deadline:
                    timeout = deadline - time.time()
                    if timeout <= 0:
                        break

                    try:
                        request = self.commit_queue.get(timeout=timeout)
                        commits_to_process.append(request)
                    except queue.Empty:
                        break

                # Process collected commits
                if commits_to_process:
                    self._process_batch(commits_to_process)
                    last_commit_time = time.time()

            except Exception as e:
                # Log error and continue
                warnings.warn(f"Error in commit worker: {e}")

    def _process_batch(self, requests: List[cursor.SequencedCommitCursorRequest]):
        """Process a batch of commit requests."""
        # Group by partition for efficient commit
        partition_offsets = {}

        for request in requests:
            partition = request.cursor.offset // 1000000  # Extract partition from cursor
            offset = request.cursor.offset % 1000000  # Extract offset from cursor

            if partition not in partition_offsets or offset > partition_offsets[partition]:
                partition_offsets[partition] = offset

        # Commit all offsets
        try:
            offsets = []
            for partition, offset in partition_offsets.items():
                tp = TopicPartition(self.topic, partition, offset + 1)
                offsets.append(tp)

            if offsets:
                self.consumer.commit(offsets=offsets, asynchronous=False)

            # Send acknowledgments
            for _ in requests:
                self.acknowledged_count += 1
                response = cursor.SequencedCommitCursorResponse(
                    acknowledged_commits=self.acknowledged_count
                )
                self.response_queue.put(response)

        except KafkaException as e:
            # Send error responses
            for _ in requests:
                self.response_queue.put(
                    core_exceptions.InternalServerError(f"Kafka commit failed: {e}")
                )


class CursorServiceKafkaTransport(CursorServiceTransport):
    """Kafka transport class for CursorService."""

    def __init__(
        self,
        *,
        host: str = None,
        credentials: Optional[ga_credentials.Credentials] = None,
        consumer_config: Optional[Dict] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
        **kwargs,
    ) -> None:
        """Instantiate the transport.

        Args:
            host: Ignored for Kafka transport
            credentials: OAuth credentials for Kafka
            consumer_config: Kafka consumer configuration dict
            client_info: Client info for user agent
        """
        if not HAS_KAFKA:
            raise ImportError(
                "confluent-kafka is required for Kafka transport. "
                "Install it with: pip install google-cloud-pubsublite[kafka]"
            )

        # Initialize base transport
        super().__init__(
            host=host or "kafka://localhost",
            credentials=credentials,
            client_info=client_info,
            **kwargs,
        )

        # Store configuration
        self.consumer_config = consumer_config or {}
        self._consumers = {}  # Cache consumers per consumer group
        self._streaming_handlers = {}  # Active streaming handlers
        self._lock = threading.Lock()

        # Allow topic override for subscriptions (since Kafka doesn't have subscription metadata)
        self._subscription_topics = {}
        # Default topic mapping (can be overridden)
        self._default_topic = consumer_config.get('default_topic', 'test-topic') if consumer_config else 'test-topic'

        # Set up wrapped methods
        self._prep_wrapped_messages(client_info)

    def _get_consumer(self, consumer_group: str) -> "Consumer":
        """Get or create a consumer for a consumer group.

        Args:
            consumer_group: Consumer group ID

        Returns:
            Kafka Consumer instance
        """
        with self._lock:
            if consumer_group not in self._consumers:
                config = self.consumer_config.copy()

                # Remove default_topic as it's not a valid Kafka config parameter
                if 'default_topic' in config:
                    del config['default_topic']

                # Override group.id with the specific consumer group
                config['group.id'] = consumer_group

                # Ensure manual commit mode
                config['enable.auto.commit'] = False

                # Set offset reset if not already configured
                if 'auto.offset.reset' not in config:
                    config['auto.offset.reset'] = 'earliest'

                print(f"[DEBUG] Creating consumer with config:")
                print(f"  - bootstrap.servers: {config.get('bootstrap.servers', 'NOT SET')}")
                print(f"  - group.id: {config.get('group.id', 'NOT SET')}")
                print(f"  - security.protocol: {config.get('security.protocol', 'NOT SET')}")
                print(f"  - sasl.mechanisms: {config.get('sasl.mechanisms', 'NOT SET')}")
                print(f"  - oauth_cb present: {'oauth_cb' in config}")

                try:
                    self._consumers[consumer_group] = Consumer(config)
                    print(f"[DEBUG] Consumer created successfully")
                except Exception as e:
                    print(f"[ERROR] Failed to create consumer: {e}")
                    raise

            return self._consumers[consumer_group]

    def _extract_consumer_group(self, subscription_path: str) -> str:
        """Extract consumer group ID from subscription path.

        Args:
            subscription_path: Full subscription resource path

        Returns:
            Consumer group ID
        """
        # Parse subscription path: projects/{project}/locations/{location}/subscriptions/{subscription}
        parts = subscription_path.split('/')
        if len(parts) >= 6 and parts[-2] == 'subscriptions':
            return f"pubsublite-cursor-{parts[-1]}"
        return subscription_path

    def _get_topic_from_subscription(self, subscription_path: str) -> str:
        """Extract topic name from subscription path.

        Args:
            subscription_path: Full subscription resource path

        Returns:
            Topic name
        """
        # Check if there's a specific topic mapping for this subscription
        if subscription_path in self._subscription_topics:
            return self._subscription_topics[subscription_path]

        # Use default topic from config
        # In Kafka, subscriptions (consumer groups) don't inherently map to topics
        # This would normally come from subscription metadata in Pub/Sub Lite
        return self._default_topic

    def streaming_commit_cursor(
        self,
    ) -> Callable[
        [Iterator[cursor.StreamingCommitCursorRequest]],
        Iterator[cursor.StreamingCommitCursorResponse],
    ]:
        """Return callable for streaming_commit_cursor.

        Returns:
            Callable that handles streaming commit cursor requests
        """
        def stream_handler(
            requests: Iterator[cursor.StreamingCommitCursorRequest]
        ) -> Iterator[cursor.StreamingCommitCursorResponse]:
            """Handle streaming commit cursor requests.

            Args:
                requests: Iterator of streaming requests

            Yields:
                Streaming responses
            """
            handler = None
            consumer_group = None
            partition = None

            try:
                for request in requests:
                    if request.initial:
                        # Handle initial request
                        consumer_group = self._extract_consumer_group(
                            request.initial.subscription
                        )
                        partition = request.initial.partition
                        topic = self._get_topic_from_subscription(
                            request.initial.subscription
                        )

                        # Get consumer and create handler
                        consumer = self._get_consumer(consumer_group)
                        handler = StreamingCommitHandler(consumer, topic)
                        handler.start()

                        # Store handler
                        handler_key = f"{consumer_group}:{partition}"
                        with self._lock:
                            self._streaming_handlers[handler_key] = handler

                        # Send initial response
                        yield cursor.StreamingCommitCursorResponse(
                            initial=cursor.InitialCommitCursorResponse()
                        )

                    elif request.commit and handler:
                        # Handle commit request
                        handler.add_commit(request.commit)

                        # Get and yield response
                        response = handler.get_response()
                        yield cursor.StreamingCommitCursorResponse(
                            commit=response
                        )

            finally:
                # Clean up handler
                if handler:
                    handler.stop()
                    if consumer_group and partition is not None:
                        handler_key = f"{consumer_group}:{partition}"
                        with self._lock:
                            self._streaming_handlers.pop(handler_key, None)

        return stream_handler

    def commit_cursor(
        self,
    ) -> Callable[
        [cursor.CommitCursorRequest],
        cursor.CommitCursorResponse,
    ]:
        """Return callable for commit_cursor.

        Returns:
            Callable that handles commit cursor requests
        """
        def _commit_cursor(
            request: cursor.CommitCursorRequest,
        ) -> cursor.CommitCursorResponse:
            """Commit a cursor position.

            Args:
                request: Commit cursor request

            Returns:
                Commit cursor response
            """
            try:
                # Extract consumer group and topic
                consumer_group = self._extract_consumer_group(request.subscription)
                topic = self._get_topic_from_subscription(request.subscription)

                print(f"[DEBUG] Commit cursor - Group: {consumer_group}, Topic: {topic}, "
                      f"Partition: {request.partition}, Offset: {request.cursor.offset}")

                # Get consumer
                consumer = self._get_consumer(consumer_group)

                # Subscribe if needed
                current_subscription = consumer.subscription()
                print(f"[DEBUG] Current subscription: {current_subscription}")
                if not current_subscription or topic not in current_subscription:
                    print(f"[DEBUG] Subscribing to topic: {topic}")
                    try:
                        consumer.subscribe([topic])
                        print(f"[DEBUG] Subscribe successful")
                    except Exception as e:
                        print(f"[ERROR] Subscribe failed: {e}")
                        raise

                    # Poll briefly to ensure subscription takes effect
                    print(f"[DEBUG] Polling to ensure subscription...")
                    msg = consumer.poll(timeout=1.0)
                    if msg is not None:
                        if msg.error():
                            print(f"[DEBUG] Poll returned error: {msg.error()}")
                        else:
                            print(f"[DEBUG] Poll returned message from partition {msg.partition()}")
                    else:
                        print(f"[DEBUG] Poll returned no message (normal for empty topic or offset at end)")

                # Create TopicPartition with offset
                # Kafka expects the next offset to read, so add 1
                tp = TopicPartition(
                    topic=topic,
                    partition=request.partition,
                    offset=request.cursor.offset + 1
                )

                print(f"[DEBUG] Committing TopicPartition: topic={tp.topic}, partition={tp.partition}, offset={tp.offset}")

                # Commit synchronously
                consumer.commit(offsets=[tp], asynchronous=False)
                print(f"[DEBUG] Commit successful")

                return cursor.CommitCursorResponse()

            except KafkaException as e:
                print(f"[ERROR] KafkaException during commit: {e}")
                print(f"[ERROR] Error code: {e.args[0].code() if e.args else 'N/A'}")
                print(f"[ERROR] Error name: {e.args[0].name() if e.args else 'N/A'}")
                raise core_exceptions.InternalServerError(f"Kafka commit failed: {e}")
            except Exception as e:
                print(f"[ERROR] Unexpected exception during commit: {type(e).__name__}: {e}")
                raise

        return _commit_cursor

    def list_partition_cursors(
        self,
    ) -> Callable[
        [cursor.ListPartitionCursorsRequest],
        cursor.ListPartitionCursorsResponse,
    ]:
        """Return callable for list_partition_cursors.

        Returns:
            Callable that handles list partition cursors requests
        """
        def _list_partition_cursors(
            request: cursor.ListPartitionCursorsRequest,
        ) -> cursor.ListPartitionCursorsResponse:
            """List committed cursors for all partitions.

            Args:
                request: List partition cursors request

            Returns:
                List partition cursors response
            """
            try:
                # Extract consumer group and topic
                consumer_group = self._extract_consumer_group(request.parent)
                topic = self._get_topic_from_subscription(request.parent)

                # Get consumer
                consumer = self._get_consumer(consumer_group)

                # Subscribe to the topic to ensure the consumer is part of the group
                # This is needed for committed() to work properly
                current_subscription = consumer.subscription()
                if not current_subscription or topic not in current_subscription:
                    consumer.subscribe([topic])
                    # Poll briefly to ensure subscription takes effect
                    consumer.poll(timeout=0.1)

                # Get topic metadata to find all partitions
                metadata = consumer.list_topics(topic, timeout=10)
                if topic not in metadata.topics:
                    return cursor.ListPartitionCursorsResponse(
                        partition_cursors=[],
                        next_page_token=""
                    )

                topic_metadata = metadata.topics[topic]
                partitions = list(topic_metadata.partitions.keys())

                # Create TopicPartition objects for all partitions
                topic_partitions = [
                    TopicPartition(topic, p) for p in partitions
                ]

                # Get committed offsets
                committed = consumer.committed(topic_partitions, timeout=10)

                # Build response
                partition_cursors = []
                for tp in committed:
                    # Handle different offset states
                    if tp.offset == -1001:  # OFFSET_INVALID - no committed offset
                        continue
                    elif tp.offset is not None and tp.offset >= 0:
                        # Kafka offset is next to read, cursor is last read
                        cursor_offset = tp.offset - 1 if tp.offset > 0 else 0
                        partition_cursors.append(
                            cursor.PartitionCursor(
                                partition=tp.partition,
                                cursor=common.Cursor(offset=cursor_offset)
                            )
                        )

                # Handle pagination (client-side since Kafka returns all)
                page_size = request.page_size if request.page_size > 0 else len(partition_cursors)
                page_token = request.page_token

                start_idx = 0
                if page_token:
                    try:
                        start_idx = int(page_token)
                    except ValueError:
                        pass

                end_idx = min(start_idx + page_size, len(partition_cursors))
                page_cursors = partition_cursors[start_idx:end_idx]

                next_token = ""
                if end_idx < len(partition_cursors):
                    next_token = str(end_idx)

                return cursor.ListPartitionCursorsResponse(
                    partition_cursors=page_cursors,
                    next_page_token=next_token
                )

            except KafkaException as e:
                raise core_exceptions.InternalServerError(
                    f"Kafka list cursors failed: {e}"
                )

        return _list_partition_cursors

    def close(self):
        """Close all consumers and clean up resources."""
        with self._lock:
            # Stop all streaming handlers
            for handler in self._streaming_handlers.values():
                handler.stop()
            self._streaming_handlers.clear()

            # Close all consumers
            for consumer in self._consumers.values():
                try:
                    consumer.close()
                except Exception:
                    pass
            self._consumers.clear()

    @property
    def kind(self) -> str:
        """Return the transport type."""
        return "kafka"


__all__ = ("CursorServiceKafkaTransport",)