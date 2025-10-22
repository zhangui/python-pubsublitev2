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

import asyncio
import logging
from typing import List, Dict, Optional, Any

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.pubsub_v1.subscriber.message import Message
from google.pubsub_v1 import PubsubMessage
from google.protobuf.timestamp_pb2 import Timestamp

from google.cloud.pubsublite.cloudpubsub.internal.single_subscriber import (
    AsyncSingleSubscriber,
)
from google.cloud.pubsublite.cloudpubsub.internal.wrapped_message import (
    WrappedMessage,
    AckId,
)
from google.cloud.pubsublite.types import FlowControlSettings

try:
    from confluent_kafka import Consumer, KafkaError, TopicPartition
except ImportError:
    Consumer = None
    KafkaError = None
    TopicPartition = None

logger = logging.getLogger(__name__)


class AsyncKafkaSubscriber(AsyncSingleSubscriber):
    """
    Kafka implementation of AsyncSingleSubscriber that consumes from
    Google Managed Service for Apache Kafka.
    """

    def __init__(
        self,
        kafka_config: Dict[str, Any],
        topic_name: str,
        consumer_group: str,
        flow_control_settings: Optional[FlowControlSettings] = None,
    ):
        """
        Initialize the Kafka subscriber.

        Args:
            kafka_config: Kafka consumer configuration dict
            topic_name: Name of the Kafka topic to consume from
            consumer_group: Consumer group ID
            flow_control_settings: Flow control settings for message consumption
        """
        if Consumer is None:
            raise ImportError(
                "confluent-kafka is required for Kafka functionality. "
                "Install it with: pip install google-cloud-pubsublite[kafka]"
            )

        self._kafka_config = kafka_config
        self._topic_name = topic_name
        self._consumer_group = consumer_group
        self._flow_control = flow_control_settings or FlowControlSettings(
            messages_outstanding=1000,
            bytes_outstanding=1000 * 1024 * 1024,  # 1GB
        )
        self._consumer: Optional[Consumer] = None
        self._started = False
        self._pending_acks: Dict[AckId, Dict[str, Any]] = {}  # ack_id -> dict with topic, partition, offset

    def _create_consumer_config(self) -> Dict[str, Any]:
        """Create the confluent-kafka Consumer configuration."""
        config = self._kafka_config.copy()

        # Override with consumer-specific settings
        # IMPORTANT: Override group.id with the one we want to use
        config.update({
            'group.id': self._consumer_group,
            'enable.auto.commit': False,  # Manual commit on ack()
        })

        # Set auto.offset.reset if not already set
        if 'auto.offset.reset' not in config:
            config['auto.offset.reset'] = 'latest'

        return config

    def _kafka_to_pubsub_message(self, kafka_msg) -> Message:
        """Convert Kafka message to Cloud Pub/Sub Message format."""
        # Extract attributes from headers
        attributes = {}
        if kafka_msg.headers():
            for key, value in kafka_msg.headers():
                if isinstance(value, bytes):
                    attributes[key] = value.decode('utf-8', errors='ignore')
                else:
                    attributes[key] = str(value)

        # Create timestamp
        timestamp = Timestamp()
        if kafka_msg.timestamp()[1] > 0:  # timestamp_type, timestamp_value
            timestamp.FromMilliseconds(kafka_msg.timestamp()[1])
        else:
            timestamp.GetCurrentTime()

        # Create PubsubMessage
        pubsub_msg = PubsubMessage(
            data=kafka_msg.value() or b'',
            attributes=attributes,
            publish_time=timestamp,
            ordering_key=kafka_msg.key().decode('utf-8') if kafka_msg.key() else "",
        )

        # Create ack_id using AckId namedtuple (generation=0 for Kafka since we don't have resets)
        ack_id = AckId(generation=0, offset=kafka_msg.offset())

        # Store for later ack/nack
        self._pending_acks[ack_id] = {
            'topic': kafka_msg.topic(),
            'partition': kafka_msg.partition(),
            'offset': kafka_msg.offset(),
        }

        # Create wrapped message with single ack handler
        # ack_handler takes (AckId, bool) where bool is True for ack, False for nack
        wrapped = WrappedMessage(
            pb=pubsub_msg._pb,
            ack_id=ack_id,
            ack_handler=lambda id, ack: self._handle_ack(id, ack),
        )

        # WrappedMessage already inherits from Message, so return it directly
        return wrapped

    def _handle_ack(self, ack_id: AckId, should_ack: bool):
        """Handle message acknowledgment.

        Args:
            ack_id: The AckId of the message
            should_ack: True to acknowledge (commit), False to nack (don't commit)
        """
        if ack_id not in self._pending_acks:
            logger.warning(f"Ack ID {ack_id} not found in pending acks")
            return

        ack_info = self._pending_acks.pop(ack_id)

        if should_ack:
            # Commit the offset + 1 (next message to read)
            tp = TopicPartition(ack_info['topic'], ack_info['partition'], ack_info['offset'] + 1)
            self._consumer.commit(offsets=[tp], asynchronous=False)
            logger.debug(f"Committed offset {ack_info['offset'] + 1} for partition {ack_info['partition']}")
        else:
            # Nack - just don't commit, message will be redelivered on restart
            logger.debug(f"Nacked message {ack_id} - offset not committed")

    async def read(self) -> List[Message]:
        """
        Read a batch of messages from Kafka.

        Returns:
            List of Cloud Pub/Sub compatible Message objects.
        """
        if not self._started:
            raise GoogleAPICallError("Subscriber not started. Use async with statement.")

        messages = []
        bytes_read = 0
        max_messages = self._flow_control.messages_outstanding
        max_bytes = self._flow_control.bytes_outstanding

        # Poll for messages with timeout
        while len(messages) < max_messages and bytes_read < max_bytes:
            # Poll for a single message with short timeout
            kafka_msg = self._consumer.poll(timeout=0.1)

            if kafka_msg is None:
                # No more messages available right now
                if messages:
                    break  # Return what we have
                # If no messages yet, keep trying with async sleep
                await asyncio.sleep(0.01)
                continue

            if kafka_msg.error():
                if kafka_msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, normal condition
                    break
                else:
                    raise GoogleAPICallError(f"Kafka error: {kafka_msg.error()}")

            # Convert and add message
            try:
                pubsub_msg = self._kafka_to_pubsub_message(kafka_msg)
                messages.append(pubsub_msg)
                bytes_read += len(kafka_msg.value() or b'')
            except Exception as e:
                logger.error(f"Error converting Kafka message: {e}")
                continue

        return messages

    async def __aenter__(self):
        """Start the Kafka consumer."""
        if self._started:
            return self

        try:
            # Create consumer with configuration
            config = self._create_consumer_config()
            logger.info(f"Creating Kafka consumer with config: {config}")
            self._consumer = Consumer(config)

            # Subscribe to topic
            self._consumer.subscribe([self._topic_name])

            self._started = True
            logger.info(f"Kafka consumer started for topic: {self._topic_name}, group: {self._consumer_group}")
            return self
        except Exception as e:
            raise GoogleAPICallError(f"Failed to start Kafka consumer: {e}")

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the Kafka consumer and commit pending offsets."""
        if not self._started:
            return

        try:
            if self._consumer:
                # Commit any pending offsets
                self._consumer.commit(asynchronous=False)

                # Close consumer
                self._consumer.close()
                self._consumer = None

            self._started = False
            logger.info(f"Kafka consumer stopped for topic: {self._topic_name}")
        except Exception as e:
            logger.error(f"Error stopping Kafka consumer: {e}")