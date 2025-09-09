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

"""Tests for backend selection in make_publisher."""

from unittest.mock import patch, MagicMock
import pytest

# Skip all tests if Kafka is not available
pytest.importorskip("confluent_kafka")

from google.cloud.pubsublite.cloudpubsub.internal.make_publisher import (
    make_async_publisher,
)
from google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher import (
    AsyncKafkaPublisher,
)
from google.cloud.pubsublite.cloudpubsub.internal.async_publisher_impl import (
    AsyncSinglePublisherImpl,
)
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import KafkaConfig
from google.cloud.pubsublite.types import TopicPath


@pytest.fixture()
def topic_path():
    """Test topic path."""
    return TopicPath.parse("projects/test-project/locations/us-central1-a/topics/test-topic")


@pytest.fixture()
def kafka_config():
    """Test Kafka configuration."""
    return KafkaConfig(
        credentials=None,
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )


@pytest.mark.parametrize("use_kafka", [True, False])
def test_make_async_publisher_backend_selection(use_kafka, topic_path, kafka_config):
    """Test that make_async_publisher returns correct implementation based on use_kafka."""
    with patch('google.cloud.pubsublite.cloudpubsub.internal.make_publisher.make_wire_publisher') as mock_wire:
        with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer'):
            mock_wire.return_value = MagicMock()
            
            if use_kafka:
                publisher = make_async_publisher(
                    topic=topic_path,
                    transport="grpc_asyncio",
                    use_kafka=True,
                    kafka_config=kafka_config
                )
                # Should return AsyncKafkaPublisher for Kafka backend
                assert isinstance(publisher, AsyncKafkaPublisher)
                # Wire publisher should not be called for Kafka
                mock_wire.assert_not_called()
            else:
                publisher = make_async_publisher(
                    topic=topic_path,
                    transport="grpc_asyncio",
                    use_kafka=False,
                    kafka_config=None
                )
                # Should return AsyncSinglePublisherImpl for Pub/Sub Lite backend
                assert isinstance(publisher, AsyncSinglePublisherImpl)
                # Wire publisher should be called for Pub/Sub Lite
                mock_wire.assert_called_once()


def test_kafka_requires_config():
    """Test that Kafka backend requires kafka_config."""
    topic_path = TopicPath.parse("projects/test/locations/us-central1/topics/test")
    
    with pytest.raises(ValueError, match="kafka_config must be provided"):
        make_async_publisher(
            topic=topic_path,
            transport="grpc_asyncio",
            use_kafka=True,
            kafka_config=None
        )


def test_kafka_not_available_error():
    """Test error when confluent-kafka is not installed."""
    topic_path = TopicPath.parse("projects/test/locations/us-central1/topics/test")
    kafka_config = KafkaConfig(producer_config={'bootstrap.servers': 'localhost:9092'})
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.make_publisher.KAFKA_AVAILABLE', False):
        with pytest.raises(ImportError, match="confluent-kafka is not installed"):
            make_async_publisher(
                topic=topic_path,
                transport="grpc_asyncio",
                use_kafka=True,
                kafka_config=kafka_config
            )


def test_pubsublite_backend_ignores_kafka_config(topic_path):
    """Test that Pub/Sub Lite backend ignores kafka_config parameter."""
    with patch('google.cloud.pubsublite.cloudpubsub.internal.make_publisher.make_wire_publisher') as mock_wire:
        mock_wire.return_value = MagicMock()
        
        # Even if kafka_config is provided, it should be ignored for Pub/Sub Lite
        publisher = make_async_publisher(
            topic=topic_path,
            transport="grpc_asyncio",
            use_kafka=False,
            kafka_config=KafkaConfig(producer_config={'bootstrap.servers': 'ignored'})
        )
        
        assert isinstance(publisher, AsyncSinglePublisherImpl)
        mock_wire.assert_called_once()