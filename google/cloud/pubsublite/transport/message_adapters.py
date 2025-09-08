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

from typing import Optional, Dict, Any, List
import json
import base64
from dataclasses import dataclass

from google.cloud.pubsub_v1.types import PubsubMessage
from google.cloud.pubsub_v1.subscriber.message import Message


@dataclass
class KafkaMessage:
    """Representation of a Kafka message."""
    topic: str
    partition: Optional[int] = None
    key: Optional[str] = None
    value: Optional[bytes] = None
    headers: Optional[Dict[str, str]] = None
    timestamp: Optional[int] = None
    offset: Optional[int] = None


class MessageAdapter:
    """Adapter for converting between Pub/Sub and Kafka message formats."""
    
    # Special attributes that map to Kafka message properties
    ORDERING_KEY_ATTR = "ordering_key"
    PARTITION_KEY_ATTR = "kafka_partition_key"  
    PARTITION_ATTR = "kafka_partition"
    TIMESTAMP_ATTR = "publish_time"
    
    # Header prefix for Pub/Sub attributes in Kafka
    PUBSUB_ATTR_PREFIX = "pubsub_attr_"
    
    @staticmethod
    def pubsub_to_kafka(
        pubsub_msg: PubsubMessage,
        topic: str,
        partition: Optional[int] = None,
    ) -> KafkaMessage:
        """Convert a Pub/Sub message to Kafka format.
        
        Args:
            pubsub_msg: The Pub/Sub message to convert.
            topic: Target Kafka topic name.
            partition: Optional target partition (overrides key-based routing).
            
        Returns:
            Kafka message representation.
        """
        # Extract key from ordering_key or partition key attribute
        key = None
        if pubsub_msg.ordering_key:
            key = pubsub_msg.ordering_key
        elif MessageAdapter.PARTITION_KEY_ATTR in pubsub_msg.attributes:
            key = pubsub_msg.attributes[MessageAdapter.PARTITION_KEY_ATTR]
        
        # Extract partition if specified in attributes
        if partition is None and MessageAdapter.PARTITION_ATTR in pubsub_msg.attributes:
            try:
                partition = int(pubsub_msg.attributes[MessageAdapter.PARTITION_ATTR])
            except ValueError:
                # Ignore invalid partition values
                pass
        
        # Convert attributes to headers
        headers = {}
        for attr_key, attr_value in pubsub_msg.attributes.items():
            # Skip special attributes that are mapped to Kafka properties
            if attr_key in [
                MessageAdapter.PARTITION_KEY_ATTR,
                MessageAdapter.PARTITION_ATTR,
                MessageAdapter.TIMESTAMP_ATTR
            ]:
                continue
            
            # Add with prefix to avoid conflicts with Kafka headers
            headers[f"{MessageAdapter.PUBSUB_ATTR_PREFIX}{attr_key}"] = attr_value
        
        # Extract timestamp
        timestamp = None
        if MessageAdapter.TIMESTAMP_ATTR in pubsub_msg.attributes:
            try:
                timestamp = int(pubsub_msg.attributes[MessageAdapter.TIMESTAMP_ATTR])
            except ValueError:
                pass
        elif hasattr(pubsub_msg, 'publish_time') and pubsub_msg.publish_time:
            timestamp = int(pubsub_msg.publish_time.timestamp() * 1000)
        
        return KafkaMessage(
            topic=topic,
            partition=partition,
            key=key,
            value=pubsub_msg.data,
            headers=headers,
            timestamp=timestamp
        )
    
    @staticmethod
    def kafka_to_pubsub(
        kafka_msg: KafkaMessage,
        message_id: Optional[str] = None,
        ack_id: Optional[str] = None,
    ) -> Message:
        """Convert a Kafka message to Pub/Sub format.
        
        Args:
            kafka_msg: The Kafka message to convert.
            message_id: Message ID for the Pub/Sub message.
            ack_id: Ack ID for the Pub/Sub message.
            
        Returns:
            Pub/Sub message representation.
        """
        # Build attributes from headers
        attributes = {}
        
        # Extract Pub/Sub attributes from headers
        if kafka_msg.headers:
            for header_key, header_value in kafka_msg.headers.items():
                if header_key.startswith(MessageAdapter.PUBSUB_ATTR_PREFIX):
                    # Remove prefix to get original attribute name
                    attr_key = header_key[len(MessageAdapter.PUBSUB_ATTR_PREFIX):]
                    attributes[attr_key] = header_value
        
        # Add Kafka-specific metadata as attributes
        if kafka_msg.key:
            attributes[MessageAdapter.ORDERING_KEY_ATTR] = kafka_msg.key
        
        if kafka_msg.partition is not None:
            attributes[MessageAdapter.PARTITION_ATTR] = str(kafka_msg.partition)
        
        if kafka_msg.timestamp is not None:
            attributes[MessageAdapter.TIMESTAMP_ATTR] = str(kafka_msg.timestamp)
        
        if kafka_msg.offset is not None:
            attributes["kafka_offset"] = str(kafka_msg.offset)
        
        # Create PubsubMessage with proper publish_time
        from google.protobuf.timestamp_pb2 import Timestamp
        import time
        
        publish_time = Timestamp()
        if kafka_msg.timestamp:
            publish_time.FromMilliseconds(kafka_msg.timestamp)
        else:
            # Use current time if no timestamp available
            publish_time.FromSeconds(int(time.time()))
        
        pubsub_msg = PubsubMessage(
            data=kafka_msg.value or b'',
            attributes=attributes,
            ordering_key=kafka_msg.key or "",
            message_id=message_id or "",
            publish_time=publish_time,
        )
        
        # For testing and basic functionality, return the PubsubMessage directly
        # In a full implementation, you would wrap this in a proper Message class
        # that handles Kafka-specific ack/nack semantics
        
        # Create a simple message wrapper that mimics the Message interface
        class SimpleMessage:
            def __init__(self, pubsub_message, message_id="", ack_id=""):
                self._pubsub_message = pubsub_message
                self.message_id = message_id
                self.ack_id = ack_id
            
            @property
            def data(self):
                return self._pubsub_message.data
            
            @property
            def attributes(self):
                return self._pubsub_message.attributes
            
            @property
            def ordering_key(self):
                return self._pubsub_message.ordering_key
            
            @property
            def publish_time(self):
                return self._pubsub_message.publish_time
            
            def ack(self):
                # Placeholder - would commit Kafka offset in real implementation
                pass
            
            def nack(self):
                # Placeholder - would handle nack in real implementation
                pass
        
        return SimpleMessage(pubsub_msg, message_id or "", ack_id or "")
    
    @staticmethod
    def batch_pubsub_to_kafka(
        messages: List[PubsubMessage],
        topic: str,
    ) -> List[KafkaMessage]:
        """Convert a batch of Pub/Sub messages to Kafka format.
        
        Args:
            messages: List of Pub/Sub messages to convert.
            topic: Target Kafka topic name.
            
        Returns:
            List of Kafka messages.
        """
        return [
            MessageAdapter.pubsub_to_kafka(msg, topic)
            for msg in messages
        ]
    
    @staticmethod
    def batch_kafka_to_pubsub(
        messages: List[KafkaMessage],
    ) -> List[Message]:
        """Convert a batch of Kafka messages to Pub/Sub format.
        
        Args:
            messages: List of Kafka messages to convert.
            
        Returns:
            List of Pub/Sub messages.
        """
        return [
            MessageAdapter.kafka_to_pubsub(
                msg,
                message_id=f"{msg.topic}:{msg.partition}:{msg.offset}" if msg.offset else None
            )
            for msg in messages
        ]
    
    @staticmethod
    def create_kafka_message_from_dict(data: Dict[str, Any]) -> KafkaMessage:
        """Create a KafkaMessage from a dictionary representation.
        
        Useful for testing and serialization.
        
        Args:
            data: Dictionary containing message data.
            
        Returns:
            KafkaMessage instance.
        """
        # Handle base64-encoded value
        value = data.get('value')
        if isinstance(value, str):
            try:
                value = base64.b64decode(value)
            except Exception:
                value = value.encode('utf-8')
        
        return KafkaMessage(
            topic=data['topic'],
            partition=data.get('partition'),
            key=data.get('key'),
            value=value,
            headers=data.get('headers'),
            timestamp=data.get('timestamp'),
            offset=data.get('offset')
        )
    
    @staticmethod
    def kafka_message_to_dict(message: KafkaMessage) -> Dict[str, Any]:
        """Convert a KafkaMessage to dictionary representation.
        
        Useful for testing and serialization.
        
        Args:
            message: KafkaMessage to convert.
            
        Returns:
            Dictionary representation.
        """
        # Base64 encode binary value for JSON serialization
        value = message.value
        if isinstance(value, bytes):
            value = base64.b64encode(value).decode('ascii')
        
        return {
            'topic': message.topic,
            'partition': message.partition,
            'key': message.key,
            'value': value,
            'headers': message.headers,
            'timestamp': message.timestamp,
            'offset': message.offset
        }