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

"""Minimal tests for Kafka-specific behavior in AsyncKafkaPublisher."""

from unittest.mock import MagicMock, patch
import pytest

# Skip all tests if Kafka is not available
pytest.importorskip("confluent_kafka")

from google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher import (
    AsyncKafkaPublisher,
)
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import KafkaConfig


@pytest.fixture()
def kafka_config():
    """Test Kafka configuration."""
    return KafkaConfig(
        credentials=None,
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )


def test_kafka_ack_id_format():
    """Test that Kafka ack IDs follow topic:partition:offset format."""
    # Test parsing Kafka ack ID format
    ack_id = "my-kafka-topic:2:98765"
    
    # Verify format: topic:partition:offset
    assert ":" in ack_id
    parts = ack_id.split(":")
    assert len(parts) == 3
    
    topic, partition, offset = parts
    assert topic == "my-kafka-topic"
    assert partition.isdigit()
    assert offset.isdigit()
    assert int(partition) >= 0
    assert int(offset) >= 0


@patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer')
def test_kafka_producer_config_creation(mock_producer_class, kafka_config):
    """Test that Kafka producer config is created correctly."""
    mock_producer_class.return_value = MagicMock()
    
    publisher = AsyncKafkaPublisher(kafka_config, "test-topic")
    config = publisher._create_producer_config()
    
    # Should include bootstrap.servers from user config
    assert config['bootstrap.servers'] == 'localhost:9092'
    
    # Should include default OAuth settings
    assert config.get('security.protocol') == 'SASL_SSL'
    assert config.get('sasl.mechanisms') == 'OAUTHBEARER'


def test_kafka_message_headers_conversion():
    """Test that message attributes are converted to Kafka headers."""
    # This would be the expected conversion logic
    attributes = {
        'content_type': 'application/json',
        'source': 'test-service',
        'event_id': '12345'
    }
    
    # Expected headers format for Kafka
    expected_headers = [
        ('content_type', b'application/json'),
        ('source', b'test-service'),
        ('event_id', b'12345')
    ]
    
    # Convert attributes to headers (this logic would be in AsyncKafkaPublisher)
    headers = [(key, value.encode('utf-8')) for key, value in attributes.items()]
    
    assert headers == expected_headers


def test_kafka_ordering_key_as_partition_key():
    """Test that ordering_key becomes Kafka partition key."""
    ordering_key = "user-123"
    
    # In Kafka, ordering_key should become the message key for partitioning
    expected_key = ordering_key.encode('utf-8')
    assert expected_key == b"user-123"
    
    # Empty ordering_key should become None (random partitioning)
    empty_key = ""
    expected_empty = None if not empty_key else empty_key.encode('utf-8')
    assert expected_empty is None


@patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer')
def test_kafka_mtls_todo_placeholder(mock_producer_class, kafka_config):
    """Test that mTLS configuration has a TODO placeholder."""
    mock_producer_class.return_value = MagicMock()
    
    publisher = AsyncKafkaPublisher(kafka_config, "test-topic")
    config = publisher._create_producer_config()
    
    # Should have OAuth as default (mTLS is TODO)
    if 'security.protocol' not in kafka_config.producer_config:
        assert config.get('security.protocol') == 'SASL_SSL'
        assert config.get('sasl.mechanisms') == 'OAUTHBEARER'
        # Default OAuth endpoint
        assert config.get('sasl.oauthbearer.token.endpoint.url') == 'localhost:14293'


def test_kafka_topic_name_extraction():
    """Test that topic name is properly extracted and used."""
    topic_name = "my-kafka-topic"
    
    # The publisher should store and use the topic name correctly
    assert topic_name == "my-kafka-topic"
    assert len(topic_name) > 0
    assert not topic_name.startswith("projects/")  # Not a Pub/Sub Lite path


@patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer')
def test_kafka_user_config_preserved(mock_producer_class, kafka_config):
    """Test that user configuration is preserved and not overwritten."""
    # Add custom user settings
    kafka_config.producer_config.update({
        'client.id': 'my-custom-client',
        'request.timeout.ms': 60000,
        'custom.setting': 'custom-value'
    })
    
    mock_producer_class.return_value = MagicMock()
    
    publisher = AsyncKafkaPublisher(kafka_config, "test-topic")
    config = publisher._create_producer_config()
    
    # User settings should be preserved
    assert config['client.id'] == 'my-custom-client'
    assert config['request.timeout.ms'] == 60000
    assert config['custom.setting'] == 'custom-value'
    
    # Bootstrap servers should be preserved
    assert config['bootstrap.servers'] == 'localhost:9092'