# Copyright 2020 Google LLC
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

import pytest
from google.cloud.pubsub_v1.types import PubsubMessage

from google.cloud.pubsublite.transport.message_adapters import MessageAdapter, KafkaMessage


class TestMessageAdapter:
    """Tests for MessageAdapter."""
    
    def test_pubsub_to_kafka_conversion(self):
        """Test converting Pub/Sub message to Kafka format."""
        pubsub_msg = PubsubMessage(
            data=b"test message",
            attributes={
                "key1": "value1", 
                "ordering_key": "order123",
                "custom_attr": "custom_value"
            },
            ordering_key="order123"
        )
        
        kafka_msg = MessageAdapter.pubsub_to_kafka(pubsub_msg, "test-topic")
        
        assert kafka_msg.topic == "test-topic"
        assert kafka_msg.value == b"test message"
        assert kafka_msg.key == "order123"  # From ordering_key
        assert kafka_msg.headers["pubsub_attr_key1"] == "value1"
        assert kafka_msg.headers["pubsub_attr_custom_attr"] == "custom_value"
        # ordering_key should not appear in headers (it's mapped to key)
        assert "pubsub_attr_ordering_key" not in kafka_msg.headers
    
    def test_pubsub_to_kafka_with_partition_key(self):
        """Test Pub/Sub message with explicit partition key attribute."""
        pubsub_msg = PubsubMessage(
            data=b"test message",
            attributes={
                "kafka_partition_key": "explicit_key",
                "other_attr": "other_value"
            }
        )
        
        kafka_msg = MessageAdapter.pubsub_to_kafka(pubsub_msg, "test-topic")
        
        assert kafka_msg.key == "explicit_key"
        assert "pubsub_attr_other_attr" in kafka_msg.headers
        # kafka_partition_key should not appear in headers
        assert "pubsub_attr_kafka_partition_key" not in kafka_msg.headers
    
    def test_pubsub_to_kafka_with_explicit_partition(self):
        """Test Pub/Sub message with explicit partition attribute."""
        pubsub_msg = PubsubMessage(
            data=b"test message",
            attributes={"kafka_partition": "2"}
        )
        
        kafka_msg = MessageAdapter.pubsub_to_kafka(pubsub_msg, "test-topic")
        
        assert kafka_msg.partition is None  # No override provided
        
        # With explicit partition override
        kafka_msg = MessageAdapter.pubsub_to_kafka(pubsub_msg, "test-topic", partition=1)
        assert kafka_msg.partition == 1
    
    def test_kafka_to_pubsub_conversion(self):
        """Test converting Kafka message to Pub/Sub format."""
        kafka_msg = KafkaMessage(
            topic="test-topic",
            partition=1,
            key="test_key",
            value=b"test message",
            headers={
                "pubsub_attr_key1": "value1",
                "pubsub_attr_custom_attr": "custom_value",
                "non_pubsub_header": "should_be_ignored"
            },
            timestamp=1234567890000,
            offset=12345
        )
        
        pubsub_msg = MessageAdapter.kafka_to_pubsub(kafka_msg, "msg_id_123")
        
        assert pubsub_msg.data == b"test message"
        assert pubsub_msg.ordering_key == "test_key"
        assert pubsub_msg.message_id == "msg_id_123"
        
        # Check attributes
        assert pubsub_msg.attributes["key1"] == "value1"  # Prefix removed
        assert pubsub_msg.attributes["custom_attr"] == "custom_value"
        assert pubsub_msg.attributes["ordering_key"] == "test_key"
        assert pubsub_msg.attributes["kafka_partition"] == "1"
        assert pubsub_msg.attributes["kafka_offset"] == "12345"
        assert pubsub_msg.attributes["publish_time"] == "1234567890000"
        
        # Non-pubsub header should not appear
        assert "non_pubsub_header" not in pubsub_msg.attributes
    
    def test_round_trip_conversion(self):
        """Test that pubsub -> kafka -> pubsub preserves essential data."""
        original = PubsubMessage(
            data=b"round trip test",
            attributes={
                "key1": "value1",
                "key2": "value2"
            },
            ordering_key="round_trip_key"
        )
        
        # Convert to Kafka and back
        kafka_msg = MessageAdapter.pubsub_to_kafka(original, "test-topic")
        converted_back = MessageAdapter.kafka_to_pubsub(kafka_msg)
        
        # Check essential data is preserved
        assert converted_back.data == original.data
        assert converted_back.ordering_key == original.ordering_key
        
        # Original attributes should be preserved (with some additions from Kafka)
        assert converted_back.attributes["key1"] == original.attributes["key1"]
        assert converted_back.attributes["key2"] == original.attributes["key2"]
    
    def test_batch_conversion_pubsub_to_kafka(self):
        """Test batch conversion from Pub/Sub to Kafka."""
        messages = [
            PubsubMessage(data=b"msg1", ordering_key="key1"),
            PubsubMessage(data=b"msg2", ordering_key="key2"),
            PubsubMessage(data=b"msg3", ordering_key="key3"),
        ]
        
        kafka_messages = MessageAdapter.batch_pubsub_to_kafka(messages, "test-topic")
        
        assert len(kafka_messages) == 3
        assert all(msg.topic == "test-topic" for msg in kafka_messages)
        assert kafka_messages[0].value == b"msg1"
        assert kafka_messages[0].key == "key1"
        assert kafka_messages[1].value == b"msg2"
        assert kafka_messages[1].key == "key2"
        assert kafka_messages[2].value == b"msg3"
        assert kafka_messages[2].key == "key3"
    
    def test_batch_conversion_kafka_to_pubsub(self):
        """Test batch conversion from Kafka to Pub/Sub."""
        kafka_messages = [
            KafkaMessage(topic="topic1", value=b"msg1", key="key1", offset=100),
            KafkaMessage(topic="topic2", value=b"msg2", key="key2", offset=101),
            KafkaMessage(topic="topic3", value=b"msg3", key="key3", offset=102),
        ]
        
        pubsub_messages = MessageAdapter.batch_kafka_to_pubsub(kafka_messages)
        
        assert len(pubsub_messages) == 3
        assert pubsub_messages[0].data == b"msg1"
        assert pubsub_messages[0].ordering_key == "key1"
        assert pubsub_messages[0].message_id == "topic1:None:100"
        assert pubsub_messages[1].data == b"msg2"
        assert pubsub_messages[1].ordering_key == "key2"
        assert pubsub_messages[1].message_id == "topic2:None:101"
    
    def test_kafka_message_dict_serialization(self):
        """Test KafkaMessage dictionary serialization."""
        kafka_msg = KafkaMessage(
            topic="test-topic",
            partition=1,
            key="test_key",
            value=b"test message",
            headers={"header1": "value1"},
            timestamp=1234567890,
            offset=100
        )
        
        # Convert to dict
        msg_dict = MessageAdapter.kafka_message_to_dict(kafka_msg)
        
        assert msg_dict["topic"] == "test-topic"
        assert msg_dict["partition"] == 1
        assert msg_dict["key"] == "test_key"
        assert msg_dict["headers"] == {"header1": "value1"}
        assert msg_dict["timestamp"] == 1234567890
        assert msg_dict["offset"] == 100
        
        # Value should be base64 encoded
        import base64
        assert msg_dict["value"] == base64.b64encode(b"test message").decode('ascii')
        
        # Convert back from dict
        restored_msg = MessageAdapter.create_kafka_message_from_dict(msg_dict)
        
        assert restored_msg.topic == kafka_msg.topic
        assert restored_msg.partition == kafka_msg.partition
        assert restored_msg.key == kafka_msg.key
        assert restored_msg.value == kafka_msg.value
        assert restored_msg.headers == kafka_msg.headers
        assert restored_msg.timestamp == kafka_msg.timestamp
        assert restored_msg.offset == kafka_msg.offset
    
    def test_empty_message_handling(self):
        """Test handling of empty/minimal messages."""
        # Empty Pub/Sub message
        empty_pubsub = PubsubMessage()
        kafka_msg = MessageAdapter.pubsub_to_kafka(empty_pubsub, "test-topic")
        
        assert kafka_msg.topic == "test-topic"
        assert kafka_msg.value == b""
        assert kafka_msg.key is None
        assert kafka_msg.headers == {}
        
        # Empty Kafka message
        empty_kafka = KafkaMessage(topic="test-topic")
        pubsub_msg = MessageAdapter.kafka_to_pubsub(empty_kafka)
        
        assert pubsub_msg.data == b""
        assert pubsub_msg.ordering_key == ""
        assert pubsub_msg.attributes.get("kafka_partition") is None