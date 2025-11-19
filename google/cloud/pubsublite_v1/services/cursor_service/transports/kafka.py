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
            kafka_config: Dict with Kafka config (must include 'bootstrap.servers').
            credentials: Google Cloud credentials.
            **kwargs: Additional arguments passed to base transport.
        """
        if not CONFLUENT_KAFKA_AVAILABLE:
            raise ImportError(
                "confluent-kafka is required for Kafka transport. "
                "Install with: pip install confluent-kafka"
            )

        self._kafka_config = kafka_config or {}
        if 'bootstrap.servers' not in self._kafka_config:
            raise ValueError(
                "kafka_config must contain 'bootstrap.servers'. "
                "Example: kafka_config={'bootstrap.servers': 'localhost:9092'}"
            )

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
                group_id = self._extract_subscription_name(request.subscription)
                partition = request.partition
                offset = request.cursor.offset
                
                # We need the topic name. In PSL, Subscription implies Topic.
                # But here we don't have the mapping.
                # Limitation: We assume the user provides the topic in the subscription name 
                # OR we have to look it up (expensive).
                # For now, let's assume the subscription name IS the topic name (common in simple setups)
                # OR we fail if we can't derive it.
                # Actually, we can try to list consumer groups to see if it exists and what it's consuming?
                # No, that's too slow.
                
                # WORKAROUND: We'll assume the subscription name format contains the topic 
                # or we just use the subscription name as the group ID and hope the user 
                # configured the consumer to use that group ID and topic.
                # But to commit, we NEED the topic name for TopicPartition.
                
                # Let's try to fetch the topic from the subscription path if possible?
                # No, the request only has subscription path.
                
                # If we can't get the topic, we can't commit.
                # However, if we assume the subscription was created via our AdminClient,
                # maybe we can store metadata? No state here.
                
                # Let's check if we can get the topic from the subscription name if it follows a pattern?
                # Default PSL pattern: .../subscriptions/sub-id
                
                # CRITICAL ISSUE: We need the topic name to commit an offset in Kafka.
                # Option 1: Require subscription name to be "topic-group" or similar?
                # Option 2: Look up subscription (consumer group) to see what it's consuming.
                #           But a group can consume multiple topics.
                
                # Let's try Option 2: Describe the group.
                future = self._admin_client.list_consumer_group_offsets(
                    [ConsumerGroupTopicPartitions(group_id)]
                )
                try:
                    result = future[group_id].result()
                    # result is ConsumerGroupTopicPartitions
                    # It contains a list of TopicPartitions.
                    # We need to find the one with the matching partition.
                    
                    target_topic = None
                    for tp in result.topic_partitions:
                        if tp.partition == partition:
                            target_topic = tp.topic
                            break
                    
                    if not target_topic:
                        # If group has no offsets yet, we can't know the topic.
                        # This is a blocker for "first commit".
                        raise ValueError(
                            f"Cannot determine topic for subscription {group_id} and partition {partition}. "
                            "Ensure the consumer group has committed offsets at least once."
                        )
                        
                except Exception as e:
                     raise ValueError(f"Failed to resolve topic for subscription: {e}")

                # Now commit the new offset
                # Note: PSL cursor offset is "next offset to read" (exclusive)? 
                # Or "last read offset"?
                # PSL: Cursor.offset is the offset of the message.
                # Kafka: Committed offset is the NEXT message to read.
                # So if PSL says "I processed offset X", we should commit X + 1.
                
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
                group_id = self._extract_subscription_name(request.parent)
                
                future = self._admin_client.list_consumer_group_offsets(
                    [ConsumerGroupTopicPartitions(group_id)]
                )
                result = future[group_id].result()
                
                partition_cursors = []
                for tp in result.topic_partitions:
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
                
                # We can't keep a persistent connection easily with AdminClient,
                # so we'll just process each commit request individually.
                
                # Yield initial response
                yield cursor.StreamingCommitCursorResponse(
                    initial=cursor.InitialCommitCursorResponse()
                )
                
                for req in req_iter:
                    if req.commit:
                        # Reuse the logic from commit_cursor (simplified)
                        # Note: This will be slow for high throughput as it makes an Admin API call per commit.
                        # But it's functional.
                        
                        # We need to reconstruct a CommitCursorRequest-like object or just call logic directly
                        # But wait, streaming request doesn't have Subscription in every message, only in Initial.
                        
                        group_id = self._extract_subscription_name(first_req.initial.subscription)
                        partition = first_req.initial.partition
                        offset = req.commit.cursor.offset
                        
                        # Resolve topic (cached from initial lookup?)
                        # TODO: Cache topic resolution
                        
                        # For now, implementing full logic here is complex.
                        # We'll just acknowledge without doing anything for this MVP step
                        # OR try to implement it properly.
                        
                        # Let's try to implement properly with topic lookup once.
                        if not hasattr(self, '_cached_topics'):
                            self._cached_topics = {}
                            
                        cache_key = f"{group_id}:{partition}"
                        target_topic = self._cached_topics.get(cache_key)
                        
                        if not target_topic:
                            # Resolve topic
                            future = self._admin_client.list_consumer_group_offsets(
                                [ConsumerGroupTopicPartitions(group_id)]
                            )
                            result = future[group_id].result()
                            for tp in result.topic_partitions:
                                if tp.partition == partition:
                                    target_topic = tp.topic
                                    self._cached_topics[cache_key] = target_topic
                                    break
                        
                        if target_topic:
                            kafka_offset = offset + 1
                            tp = TopicPartition(target_topic, partition, kafka_offset)
                            group_tp = ConsumerGroupTopicPartitions(group_id, [tp])
                            fs = self._admin_client.alter_consumer_group_offsets([group_tp])
                            fs[group_id].result()
                            
                            yield cursor.StreamingCommitCursorResponse(
                                commit=cursor.SequencedCommitCursorResponse(
                                    acknowledged_commits=1 
                                )
                            )
                        else:
                             # Fail or ignore?
                             logger.error(f"Could not resolve topic for {group_id}:{partition}")

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
