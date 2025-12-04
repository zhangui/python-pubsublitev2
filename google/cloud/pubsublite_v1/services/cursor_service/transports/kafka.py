# -*- coding: utf-8 -*-
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
#
"""Kafka transport for CursorService."""

import logging
from typing import Callable, Dict, Any, Optional, Tuple, List, Iterator, Iterable
from google.cloud.pubsublite_v1.types import cursor, common
from google.protobuf import timestamp_pb2
from .base import CursorServiceTransport

logger = logging.getLogger(__name__)

try:
    from confluent_kafka.admin import AdminClient
    from confluent_kafka import (
        KafkaException,
        TopicPartition,
        ConsumerGroupTopicPartitions,
    )
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False
    AdminClient = None


class CursorServiceKafkaTransport(CursorServiceTransport):
    """Kafka transport for CursorService.

    Manages committed cursors (consumer group offsets) using the Kafka protocol.
    """

    def __init__(
        self,
        *,
        host: str = "pubsublite.googleapis.com",
        credentials=None,
        credentials_file=None,
        scopes=None,
        client_cert_source_for_mtls=None,
        quota_project_id=None,
        client_info=None,
        always_use_jwt_access=True,
        api_audience=None,
        kafka_config: Dict[str, Any] = None,
        **kwargs
    ) -> None:
        """Initialize Kafka transport.

        Args:
            kafka_config: Dict with Kafka config.
                          Can include 'topic', 'group.id', 'partition' to override request values.
                          Must include 'bootstrap.servers'.
            credentials: Google Cloud credentials.
            **kwargs: Additional arguments passed to base transport.
        """
        if not CONFLUENT_KAFKA_AVAILABLE:
            raise ImportError(
                "confluent-kafka is required for Kafka transport. "
                "Install with: pip install confluent-kafka"
            )

        # Process kafka_config
        self._kafka_config = kafka_config.copy() if kafka_config else {}
        
        if 'bootstrap.servers' not in self._kafka_config:
            raise ValueError(
                "kafka_config must contain 'bootstrap.servers'. "
                "Example: kafka_config={'bootstrap.servers': 'localhost:9092'}"
            )

        # Extract explicit overrides
        self._default_topic = self._kafka_config.pop('topic', None)
        # self._default_partition = self._kafka_config.pop('partition', None) # Removed per user request
        self._default_group_id = self._kafka_config.get('group.id') # Keep group.id in config for AdminClient? 
        # AdminClient doesn't strictly need group.id but it doesn't hurt.

        # Create Kafka AdminClient
        self._admin_client = AdminClient(self._kafka_config)
        self._stubs = {}

        super().__init__(
            host=host,
            credentials=credentials,
            credentials_file=credentials_file,
            scopes=scopes,
            quota_project_id=quota_project_id,
            client_info=client_info,
            always_use_jwt_access=always_use_jwt_access,
            api_audience=api_audience,
            **kwargs
        )

        # Initialize wrapped methods
        self._wrapped_methods = {
            self.commit_cursor: self.commit_cursor,
            self.list_partition_cursors: self.list_partition_cursors,
            self.streaming_commit_cursor: self.streaming_commit_cursor,
        }

    def _extract_subscription_name(self, path: str) -> str:
        """Extract subscription name (consumer group ID) from path."""
        return path.split('/')[-1]

    def _extract_topic_name(self, path: str) -> str:
        """Extract topic name from path."""
        return path.split('/')[-1]

    @property
    def commit_cursor(
        self,
    ) -> Callable[[cursor.CommitCursorRequest], cursor.CommitCursorResponse]:
        """Return callable for commit_cursor operation."""
        if "commit_cursor" not in self._stubs:
            def _commit_cursor(request: cursor.CommitCursorRequest, **kwargs) -> cursor.CommitCursorResponse:
                # Determine Group ID
                if self._default_group_id:
                    group_id = self._default_group_id
                else:
                    group_id = self._extract_subscription_name(request.subscription)
                
                # Determine Partition
                # Always use request partition
                partition = request.partition
                    
                offset = request.cursor.offset
                
                # Determine Topic
                target_topic = None
                
                # Strategy 0: Explicit Topic in Config
                if self._default_topic:
                    target_topic = self._default_topic
                # Strategy 1: Naming Convention "topic+group"
                elif '+' in group_id:
                    target_topic, group_id = group_id.split('+', 1)
                else:
                    # Strategy 2: Lookup existing offsets (Fallback)
                    future = self._admin_client.list_consumer_group_offsets(
                        [ConsumerGroupTopicPartitions(group_id)]
                    )
                    try:
                        result = future[group_id].result()
                        for tp in result.topic_partitions:
                            if tp.partition == partition:
                                target_topic = tp.topic
                                break
                    except Exception as e:
                        raise ValueError(f"Failed to resolve topic for subscription '{group_id}': {e}")

                    if not target_topic:
                        raise ValueError(
                            f"Cannot determine topic for subscription '{group_id}' and partition {partition}. "
                            "Please provide 'topic' in kafka_config, use 'topic+group' naming, "
                            "or ensure the consumer group has committed offsets at least once."
                        )

                # Commit the offset
                # PSL Cursor (last processed) -> Kafka Offset (next to read)
                kafka_offset = offset + 1
                
                tp = TopicPartition(target_topic, partition, kafka_offset)
                group_tp = ConsumerGroupTopicPartitions(group_id, [tp])
                
                fs = self._admin_client.alter_consumer_group_offsets([group_tp])
                fs[group_id].result()
                
                return cursor.CommitCursorResponse()

            self._stubs["commit_cursor"] = _commit_cursor

        return self._stubs["commit_cursor"]

    @property
    def list_partition_cursors(
        self,
    ) -> Callable[[cursor.ListPartitionCursorsRequest], cursor.ListPartitionCursorsResponse]:
        """Return callable for list_partition_cursors operation."""
        if "list_partition_cursors" not in self._stubs:
            def _list_partition_cursors(request: cursor.ListPartitionCursorsRequest, **kwargs) -> cursor.ListPartitionCursorsResponse:
                # Determine Group ID
                if self._default_group_id:
                    group_id = self._default_group_id
                else:
                    group_id = self._extract_subscription_name(request.parent)
                
                # Determine Topic (for filtering)
                target_topic = None
                if self._default_topic:
                    target_topic = self._default_topic
                elif '+' in group_id:
                    target_topic, group_id = group_id.split('+', 1)
                
                future = self._admin_client.list_consumer_group_offsets(
                    [ConsumerGroupTopicPartitions(group_id)]
                )
                try:
                    result = future[group_id].result()
                except Exception as e:
                    raise ValueError(f"Failed to list offsets for group '{group_id}': {e}")
                
                partition_cursors = []
                for tp in result.topic_partitions:
                    # Filter by topic if known
                    if target_topic and tp.topic != target_topic:
                        continue
                        
                    # Convert Kafka offset (next to read) back to PSL cursor (last read)
                    # PSL Cursor = Kafka Offset - 1
                    if tp.offset >= 0:
                        psl_offset = tp.offset - 1
                        partition_cursors.append(cursor.PartitionCursor(
                            partition=tp.partition,
                            cursor=common.Cursor(offset=psl_offset)
                        ))
                
                return cursor.ListPartitionCursorsResponse(partition_cursors=partition_cursors)

            self._stubs["list_partition_cursors"] = _list_partition_cursors

        return self._stubs["list_partition_cursors"]

    @property
    def streaming_commit_cursor(
        self,
    ) -> Callable[[Iterator[cursor.StreamingCommitCursorRequest]], Iterable[cursor.StreamingCommitCursorResponse]]:
        """Return callable for streaming_commit_cursor operation."""
        if "streaming_commit_cursor" not in self._stubs:
            def _streaming_commit_cursor(requests: Iterator[cursor.StreamingCommitCursorRequest], **kwargs) -> Iterable[cursor.StreamingCommitCursorResponse]:
                # Handle the stream
                # First request must be initial
                req_iter = iter(requests)
                try:
                    first_req = next(req_iter)
                except StopIteration:
                    return

                if not first_req.initial:
                    raise ValueError("First request must be initial")
                
                # Determine Group ID (from config or initial request)
                if self._default_group_id:
                    group_id = self._default_group_id
                else:
                    group_id = self._extract_subscription_name(first_req.initial.subscription)

                # Determine Partition (from initial request)
                partition = first_req.initial.partition
                
                # Determine Topic (once for the stream)
                target_topic = None
                
                # Strategy 0: Explicit Topic in Config
                if self._default_topic:
                    target_topic = self._default_topic
                # Strategy 1: Naming Convention "topic+group"
                elif '+' in group_id:
                    target_topic, group_id = group_id.split('+', 1)
                else:
                    # Strategy 2: Lookup existing offsets (Fallback)
                    # Try to resolve topic once
                    future = self._admin_client.list_consumer_group_offsets(
                        [ConsumerGroupTopicPartitions(group_id)]
                    )
                    try:
                        result = future[group_id].result()
                        for tp in result.topic_partitions:
                            if tp.partition == partition:
                                target_topic = tp.topic
                                break
                    except Exception as e:
                        logger.warning(f"Failed to resolve topic for subscription '{group_id}': {e}")

                if not target_topic:
                     # If we still don't have a topic, we can't commit.
                     # But maybe the user will provide it later? No, stream is established.
                     # We'll log a warning and fail on individual commits if we can't resolve it.
                     pass

                # Yield initial response
                yield cursor.StreamingCommitCursorResponse(
                    initial=cursor.InitialCommitCursorResponse()
                )
                
                for req in req_iter:
                    if req.commit:
                        if not target_topic:
                             # Try one last time or fail
                             raise ValueError(
                                f"Cannot determine topic for subscription '{group_id}' and partition {partition}. "
                                "Please provide 'topic' in kafka_config, use 'topic+group' naming, "
                                "or ensure the consumer group has committed offsets at least once."
                            )

                        offset = req.commit.cursor.offset
                        kafka_offset = offset + 1
                        
                        tp = TopicPartition(target_topic, partition, kafka_offset)
                        group_tp = ConsumerGroupTopicPartitions(group_id, [tp])
                        
                        # Note: This is still one API call per commit. 
                        # Optimization (batching) is left for future work as per plan.
                        fs = self._admin_client.alter_consumer_group_offsets([group_tp])
                        fs[group_id].result()
                        
                        yield cursor.StreamingCommitCursorResponse(
                            commit=cursor.SequencedCommitCursorResponse(
                                acknowledged_commits=1 
                            )
                        )

            self._stubs["streaming_commit_cursor"] = _streaming_commit_cursor

        return self._stubs["streaming_commit_cursor"]

    def close(self):
        """Close Kafka resources."""
        self._stubs.clear()

    @property
    def kind(self) -> str:
        """Return the transport kind."""
        return "kafka"


__all__ = ("CursorServiceKafkaTransport",)
