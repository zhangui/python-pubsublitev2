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

"""Tests for AsyncKafkaPublisher with proper confluent-kafka API."""

from unittest.mock import MagicMock, patch, AsyncMock
import pytest
import asyncio

# Skip all tests if Kafka is not available
pytest.importorskip("confluent_kafka")

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher import (
    AsyncKafkaPublisher,
)
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import KafkaConfig


@pytest.fixture()
def kafka_config():
    """Test Kafka configuration."""
    return KafkaConfig(
        credentials=None,
        producer_config={
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
        }
    )


@pytest.mark.asyncio
async def test_publish_with_proper_confluent_kafka_api():
    """Test that publish uses correct confluent-kafka Producer.produce() API."""
    mock_producer = MagicMock()
    mock_producer.produce.return_value = None  # produce() returns None on success
    mock_producer.poll.return_value = 0
    
    config = KafkaConfig(
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer',
               return_value=mock_producer):
        publisher = AsyncKafkaPublisher(config, "test-topic")
        publisher._started = True
        publisher._producer = mock_producer
        
        # Test publish
        result = await publisher.publish(
            data=b"test message",
            ordering_key="key-123",
            attr1="value1",
            attr2="value2"
        )
        
        # Verify produce was called with correct parameters
        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        
        # Check positional arguments
        assert call_args.kwargs['topic'] == "test-topic"
        assert call_args.kwargs['value'] == b"test message"
        assert call_args.kwargs['key'] == b"key-123"
        assert call_args.kwargs['timestamp'] == 0  # Current time
        
        # Check headers are list of tuples
        headers = call_args.kwargs['headers']
        assert isinstance(headers, list)
        assert all(isinstance(h, tuple) and len(h) == 2 for h in headers)
        assert ('attr1', b'value1') in headers
        assert ('attr2', b'value2') in headers
        
        # Check on_delivery callback exists
        assert 'on_delivery' in call_args.kwargs
        assert callable(call_args.kwargs['on_delivery'])
        
        # Since we don't use Futures, result should be None (what produce returns)
        assert result is None


def test_convert_attributes_to_headers():
    """Test that attributes are properly converted to Kafka headers format."""
    config = KafkaConfig(
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer'):
        publisher = AsyncKafkaPublisher(config, "test-topic")
        
        # Test conversion
        attrs = {
            'string_attr': 'test',
            'bytes_attr': b'bytes',
            'int_attr': 123,
        }
        
        headers = publisher._convert_attributes_to_headers(attrs)
        
        assert isinstance(headers, list)
        assert ('string_attr', b'test') in headers
        assert ('bytes_attr', b'bytes') in headers
        assert ('int_attr', b'123') in headers


@pytest.mark.asyncio
async def test_publish_without_started_raises_error():
    """Test that publishing without starting raises an error."""
    config = KafkaConfig(
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer'):
        publisher = AsyncKafkaPublisher(config, "test-topic")
        publisher._started = False  # Not started
        
        with pytest.raises(GoogleAPICallError, match="Publisher not started"):
            await publisher.publish(b"test")


@pytest.mark.asyncio
async def test_publish_with_buffer_error():
    """Test that BufferError is properly handled."""
    mock_producer = MagicMock()
    mock_producer.produce.side_effect = BufferError("Queue full")
    
    config = KafkaConfig(
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer',
               return_value=mock_producer):
        publisher = AsyncKafkaPublisher(config, "test-topic")
        publisher._started = True
        publisher._producer = mock_producer
        
        with pytest.raises(GoogleAPICallError, match="Kafka producer queue is full"):
            await publisher.publish(b"test message")


@pytest.mark.asyncio
async def test_async_context_manager():
    """Test that AsyncKafkaPublisher works as async context manager."""
    mock_producer = MagicMock()
    mock_producer.flush.return_value = 0
    
    config = KafkaConfig(
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer',
               return_value=mock_producer):
        async with AsyncKafkaPublisher(config, "test-topic") as publisher:
            assert publisher._started is True
            assert publisher._producer is mock_producer
        
        # After exit, should be stopped
        assert publisher._started is False
        mock_producer.flush.assert_called_once_with(30)  # 30 second timeout


def test_delivery_callback_logging():
    """Test that the delivery callback logs appropriately."""
    config = KafkaConfig(
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    mock_producer = MagicMock()
    mock_producer.produce.return_value = None
    mock_producer.poll.return_value = 0
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer',
               return_value=mock_producer):
        with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.logger') as mock_logger:
            publisher = AsyncKafkaPublisher(config, "test-topic")
            publisher._started = True
            publisher._producer = mock_producer
            
            # Get the callback from produce call
            asyncio.run(publisher.publish(b"test"))
            on_delivery = mock_producer.produce.call_args.kwargs['on_delivery']
            
            # Test error case
            mock_error = MagicMock()
            mock_error.__str__.return_value = "Test error"
            on_delivery(mock_error, None)
            mock_logger.error.assert_called_with("Failed to deliver message: Test error")
            
            # Test success case
            mock_msg = MagicMock()
            mock_msg.topic.return_value = "test-topic"
            mock_msg.partition.return_value = 0
            mock_msg.offset.return_value = 123
            on_delivery(None, mock_msg)
            mock_logger.debug.assert_called_with("Message delivered to test-topic[0]@123")


def test_empty_ordering_key_becomes_none():
    """Test that empty ordering key becomes None for Kafka."""
    config = KafkaConfig(
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    mock_producer = MagicMock()
    mock_producer.produce.return_value = None
    mock_producer.poll.return_value = 0
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer',
               return_value=mock_producer):
        publisher = AsyncKafkaPublisher(config, "test-topic")
        publisher._started = True
        publisher._producer = mock_producer
        
        # Publish with empty ordering key
        asyncio.run(publisher.publish(b"test", ordering_key=""))
        
        # Key should be None
        assert mock_producer.produce.call_args.kwargs['key'] is None
        
        # Publish with ordering key
        asyncio.run(publisher.publish(b"test", ordering_key="key123"))
        
        # Key should be encoded
        assert mock_producer.produce.call_args.kwargs['key'] == b"key123"


def test_event_time_added_as_header():
    """Test that event_time attribute is added as a special header."""
    config = KafkaConfig(
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    mock_producer = MagicMock()
    mock_producer.produce.return_value = None
    mock_producer.poll.return_value = 0
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher.Producer',
               return_value=mock_producer):
        publisher = AsyncKafkaPublisher(config, "test-topic")
        publisher._started = True
        publisher._producer = mock_producer
        
        # Publish with event_time
        asyncio.run(publisher.publish(
            b"test",
            event_time="2024-01-01T12:00:00Z"
        ))
        
        headers = mock_producer.produce.call_args.kwargs['headers']
        assert ('event_time', b'2024-01-01T12:00:00Z') in headers
        assert ('x-event-time', b'2024-01-01T12:00:00Z') in headers