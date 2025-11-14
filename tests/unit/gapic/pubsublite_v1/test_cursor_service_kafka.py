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

"""Tests for CursorService Kafka transport."""

import pytest
from unittest.mock import MagicMock, patch, Mock
from google.cloud.pubsublite_v1.services.cursor_service import CursorServiceClient
from google.cloud.pubsublite_v1.types import cursor as cursor_types
from google.cloud.pubsublite_v1.types import common


# Skip all tests if confluent_kafka is not available
pytest.importorskip("confluent_kafka")


class TestKafkaTransportRegistration:
    """Test that Kafka transport is properly registered."""

    def test_kafka_transport_in_registry(self):
        """Verify Kafka transport is registered in client."""
        from google.cloud.pubsublite_v1.services.cursor_service.client import (
            CursorServiceClient,
        )

        registry = CursorServiceClient._transport_registry
        assert "kafka" in registry, "Kafka transport should be registered"

    def test_get_kafka_transport_class(self):
        """Test getting Kafka transport class."""
        transport_class = CursorServiceClient.get_transport_class("kafka")
        assert transport_class is not None
        assert transport_class.__name__ == "CursorServiceKafkaTransport"

    def test_kafka_transport_import(self):
        """Test direct import of Kafka transport."""
        from google.cloud.pubsublite_v1.services.cursor_service.transports.kafka import (
            CursorServiceKafkaTransport,
        )

        assert CursorServiceKafkaTransport is not None


class TestCursorServiceKafkaClient:
    """Test CursorServiceClient with Kafka transport."""

    def test_create_client_with_kafka_transport(self):
        """Test creating client with Kafka transport."""
        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            "security.protocol": "PLAINTEXT",
        }

        with patch(
            "google.cloud.pubsublite_v1.services.cursor_service.transports.kafka.Consumer"
        ) as mock_consumer:
            mock_consumer.return_value = MagicMock()

            client = CursorServiceClient(
                transport="kafka", consumer_config=consumer_config
            )

            assert client is not None
            assert client._transport is not None
            assert type(client._transport).__name__ == "CursorServiceKafkaTransport"

    def test_consumer_config_passed_to_transport(self):
        """Test that consumer_config is passed to Kafka transport."""
        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
        }

        with patch(
            "google.cloud.pubsublite_v1.services.cursor_service.transports.kafka.Consumer"
        ) as mock_consumer:
            mock_consumer.return_value = MagicMock()

            client = CursorServiceClient(
                transport="kafka", consumer_config=consumer_config
            )

            # Check transport has the config
            assert hasattr(client._transport, "consumer_config")
            assert client._transport.consumer_config == consumer_config


class TestKafkaTransportConsumerManagement:
    """Test Kafka transport consumer pool management."""

    def test_consumer_pool_caching(self):
        """Test that consumers are cached per consumer group."""
        from google.cloud.pubsublite_v1.services.cursor_service.transports.kafka import (
            CursorServiceKafkaTransport,
        )

        consumer_config = {
            "bootstrap.servers": "localhost:9092",
        }

        with patch(
            "google.cloud.pubsublite_v1.services.cursor_service.transports.kafka.Consumer"
        ) as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            transport = CursorServiceKafkaTransport(consumer_config=consumer_config)

            # Get same consumer twice
            consumer1 = transport._get_consumer("test-group-1")
            consumer2 = transport._get_consumer("test-group-1")

            # Should be the same instance (cached)
            assert consumer1 is consumer2
            # Consumer should only be created once
            assert mock_consumer_class.call_count == 1

    def test_different_consumer_groups_get_different_consumers(self):
        """Test that different consumer groups get different consumer instances."""
        from google.cloud.pubsublite_v1.services.cursor_service.transports.kafka import (
            CursorServiceKafkaTransport,
        )

        consumer_config = {
            "bootstrap.servers": "localhost:9092",
        }

        with patch(
            "google.cloud.pubsublite_v1.services.cursor_service.transports.kafka.Consumer"
        ) as mock_consumer_class:
            mock_consumer_class.side_effect = [MagicMock(), MagicMock()]

            transport = CursorServiceKafkaTransport(consumer_config=consumer_config)

            consumer1 = transport._get_consumer("test-group-1")
            consumer2 = transport._get_consumer("test-group-2")

            # Should be different instances
            assert consumer1 is not consumer2
            # Consumer should be created twice
            assert mock_consumer_class.call_count == 2

    def test_consumer_config_overrides(self):
        """Test that group.id is overridden per consumer group."""
        from google.cloud.pubsublite_v1.services.cursor_service.transports.kafka import (
            CursorServiceKafkaTransport,
        )

        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "original-group",  # Should be overridden
        }

        with patch(
            "google.cloud.pubsublite_v1.services.cursor_service.transports.kafka.Consumer"
        ) as mock_consumer_class:
            mock_consumer_class.return_value = MagicMock()

            transport = CursorServiceKafkaTransport(consumer_config=consumer_config)
            transport._get_consumer("custom-group")

            # Check the config passed to Consumer()
            call_args = mock_consumer_class.call_args[0][0]
            assert call_args["group.id"] == "custom-group"
            assert call_args["bootstrap.servers"] == "localhost:9092"


class TestCommitCursorOperation:
    """Test commit_cursor operation with Kafka transport."""

    def test_commit_cursor_basic(self):
        """Test basic commit cursor operation."""
        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "default_topic": "test-topic",
        }

        mock_consumer = MagicMock()
        mock_consumer.subscription.return_value = []
        mock_consumer.commit.return_value = None

        with patch(
            "google.cloud.pubsublite_v1.services.cursor_service.transports.kafka.Consumer"
        ) as mock_consumer_class:
            mock_consumer_class.return_value = mock_consumer

            client = CursorServiceClient(
                transport="kafka", consumer_config=consumer_config
            )

            request = cursor_types.CommitCursorRequest(
                subscription="projects/test/locations/us-central1/subscriptions/test-sub",
                partition=2,
                cursor=common.Cursor(offset=100),
            )

            response = client.commit_cursor(request=request)

            assert response is not None
            # Verify consumer.commit was called
            assert mock_consumer.commit.called

    def test_commit_cursor_with_subscription(self):
        """Test that commit cursor subscribes to topic if needed."""
        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "default_topic": "test-topic",
        }

        mock_consumer = MagicMock()
        mock_consumer.subscription.return_value = None  # Not subscribed
        mock_consumer.poll.return_value = None

        with patch(
            "google.cloud.pubsublite_v1.services.cursor_service.transports.kafka.Consumer"
        ) as mock_consumer_class:
            mock_consumer_class.return_value = mock_consumer

            client = CursorServiceClient(
                transport="kafka", consumer_config=consumer_config
            )

            request = cursor_types.CommitCursorRequest(
                subscription="projects/test/locations/us-central1/subscriptions/test-sub",
                partition=0,
                cursor=common.Cursor(offset=50),
            )

            client.commit_cursor(request=request)

            # Verify consumer subscribed to topic
            mock_consumer.subscribe.assert_called_once()
            assert mock_consumer.subscribe.call_args[0][0] == ["test-topic"]


class TestListPartitionCursorsOperation:
    """Test list_partition_cursors operation with Kafka transport."""

    def test_list_partition_cursors_basic(self):
        """Test basic list partition cursors operation."""
        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "default_topic": "test-topic",
        }

        mock_consumer = MagicMock()
        mock_consumer.subscription.return_value = []
        mock_consumer.poll.return_value = None

        # Mock metadata
        mock_partition_metadata = {0: MagicMock(), 1: MagicMock(), 2: MagicMock()}
        mock_topic_metadata = MagicMock()
        mock_topic_metadata.partitions = mock_partition_metadata

        mock_metadata = MagicMock()
        mock_metadata.topics = {"test-topic": mock_topic_metadata}
        mock_consumer.list_topics.return_value = mock_metadata

        # Mock committed offsets
        from confluent_kafka import TopicPartition

        mock_committed = [
            TopicPartition("test-topic", 0, 100),
            TopicPartition("test-topic", 1, 200),
            TopicPartition("test-topic", 2, -1001),  # OFFSET_INVALID
        ]
        mock_consumer.committed.return_value = mock_committed

        with patch(
            "google.cloud.pubsublite_v1.services.cursor_service.transports.kafka.Consumer"
        ) as mock_consumer_class:
            mock_consumer_class.return_value = mock_consumer

            client = CursorServiceClient(
                transport="kafka", consumer_config=consumer_config
            )

            request = cursor_types.ListPartitionCursorsRequest(
                parent="projects/test/locations/us-central1/subscriptions/test-sub",
            )

            response = client.list_partition_cursors(request=request)

            # Should return 2 cursors (partition 2 has invalid offset)
            assert len(response.partition_cursors) == 2
            assert response.partition_cursors[0].partition == 0
            assert response.partition_cursors[0].cursor.offset == 99  # 100-1
            assert response.partition_cursors[1].partition == 1
            assert response.partition_cursors[1].cursor.offset == 199  # 200-1


class TestTopicExtraction:
    """Test topic name extraction from subscription path."""

    def test_get_topic_from_subscription(self):
        """Test extracting topic from subscription path."""
        from google.cloud.pubsublite_v1.services.cursor_service.transports.kafka import (
            CursorServiceKafkaTransport,
        )

        consumer_config = {
            "bootstrap.servers": "localhost:9092",
            "default_topic": "my-default-topic",
        }

        transport = CursorServiceKafkaTransport(consumer_config=consumer_config)

        subscription_path = "projects/test/locations/us-central1/subscriptions/test-sub"

        topic = transport._get_topic_from_subscription(subscription_path)

        # Should return default topic from config
        assert topic == "my-default-topic"

    def test_extract_consumer_group(self):
        """Test extracting consumer group from subscription path."""
        from google.cloud.pubsublite_v1.services.cursor_service.transports.kafka import (
            CursorServiceKafkaTransport,
        )

        consumer_config = {"bootstrap.servers": "localhost:9092"}

        transport = CursorServiceKafkaTransport(consumer_config=consumer_config)

        subscription_path = (
            "projects/test/locations/us-central1/subscriptions/my-subscription"
        )

        consumer_group = transport._extract_consumer_group(subscription_path)

        # Should prefix with pubsublite-cursor-
        assert consumer_group == "pubsublite-cursor-my-subscription"


class TestTransportClose:
    """Test transport cleanup."""

    def test_close_cleans_up_consumers(self):
        """Test that close() method cleans up all consumers."""
        from google.cloud.pubsublite_v1.services.cursor_service.transports.kafka import (
            CursorServiceKafkaTransport,
        )

        consumer_config = {"bootstrap.servers": "localhost:9092"}

        mock_consumer1 = MagicMock()
        mock_consumer2 = MagicMock()

        with patch(
            "google.cloud.pubsublite_v1.services.cursor_service.transports.kafka.Consumer"
        ) as mock_consumer_class:
            mock_consumer_class.side_effect = [mock_consumer1, mock_consumer2]

            transport = CursorServiceKafkaTransport(consumer_config=consumer_config)

            # Create two consumers
            transport._get_consumer("group1")
            transport._get_consumer("group2")

            # Close transport
            transport.close()

            # Verify both consumers were closed
            mock_consumer1.close.assert_called_once()
            mock_consumer2.close.assert_called_once()

            # Verify consumer cache was cleared
            assert len(transport._consumers) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
