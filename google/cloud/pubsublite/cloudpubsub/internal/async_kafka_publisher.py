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
from typing import Mapping, Optional, Dict, Any

from google.api_core.exceptions import GoogleAPICallError

from google.cloud.pubsublite.cloudpubsub.internal.single_publisher import (
    AsyncSinglePublisher,
)
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import KafkaConfig

try:
    from confluent_kafka import Producer, KafkaError
except ImportError:
    Producer = None
    KafkaError = None

logger = logging.getLogger(__name__)


class AsyncKafkaPublisher(AsyncSinglePublisher):
    """
    Kafka implementation of AsyncSinglePublisher that publishes to 
    Google Managed Service for Apache Kafka.
    
    This class implements the same interface as other AsyncSinglePublisher
    implementations, allowing it to be used seamlessly in the existing
    publisher infrastructure.
    """

    def __init__(self, kafka_config: KafkaConfig, topic_name: str):
        """
        Initialize the Kafka publisher.

        Args:
            kafka_config: Configuration for Kafka connection
            topic_name: Name of the Kafka topic (extracted from TopicPath)
        """
        if Producer is None:
            raise ImportError(
                "confluent-kafka is required for Kafka functionality. "
                "Install it with: pip install google-cloud-pubsublite[kafka]"
            )
            
        self._kafka_config = kafka_config
        self._topic_name = topic_name
        self._producer: Optional[Producer] = None
        self._started = False

    def _create_producer_config(self) -> Dict[str, Any]:
        """Create the confluent-kafka Producer configuration."""
        # Start with user-provided config which should include bootstrap.servers
        config = self._kafka_config.producer_config.copy()
        
        # TODO: Add mTLS support when available
        # For now, use OAuth authentication with default endpoint
        # if 'security.protocol' not in config:
        #     config['security.protocol'] = 'SASL_SSL'
        #     config['sasl.mechanisms'] = 'OAUTHBEARER'
        #     config['sasl.oauthbearer.token.endpoint.url'] = 'localhost:14293'  # Default OAuth endpoint
        #     config['sasl.oauthbearer.client.id'] = 'unused'
        #     config['sasl.oauthbearer.client.secret'] = 'unused'
        #     config['sasl.oauthbearer.method'] = 'oidc'
        
        # Use credentials to create token provider if OAuth is configured
        # if config.get('security.protocol') == 'SASL_SSL' and self._kafka_config.credentials:
        #     # TODO: Implement credential-based token provider
        #     # For now, this is a placeholder for when OAuth token provider is implemented
        #     pass
        
        return config

    def _create_kafka_message(
        self, 
        data: bytes, 
        ordering_key: str = "", 
        **attrs: Mapping[str, str]
    ) -> Dict[str, Any]:
        """Create a Kafka message from Pub/Sub Lite message components."""
        message = {
            'topic': self._topic_name,
            'value': data,
        }
        
        # Use ordering_key as partition key if provided
        if ordering_key:
            message['key'] = ordering_key.encode('utf-8')
        
        # Convert attributes to Kafka headers
        if attrs:
            headers = {}
            for key, value in attrs.items():
                headers[key] = value.encode('utf-8')
            message['headers'] = headers
            
        return message

    async def publish(
        self, data: bytes, ordering_key: str = "", **attrs: Mapping[str, str]
    ) -> str:
        """
        Publish a message to the Kafka topic.

        Args:
            data: The bytestring payload of the message.
            ordering_key: The key to enforce ordering on, or "" for no ordering.
            **attrs: Additional attributes to send as Kafka headers.

        Returns:
            An ack id containing topic:partition:offset.

        Raises:
            GoogleAPICallError: On a permanent failure.
        """
        if not self._started:
            raise GoogleAPICallError("Publisher not started. Use async with statement.")

        try:
            message = self._create_kafka_message(data, ordering_key, **attrs)
            
            # Create a future for the async result
            loop = asyncio.get_event_loop()
            future = loop.create_future()
            
            def delivery_callback(err, msg):
                """Callback for Kafka message delivery."""
                if err:
                    # Map Kafka errors to GoogleAPICallError
                    if not future.done():
                        error_msg = f"Kafka delivery failed: {err}"
                        future.set_exception(GoogleAPICallError(error_msg))
                else:
                    # Success - return message offset as ack ID
                    if not future.done():
                        ack_id = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
                        future.set_result(ack_id)
            
            # Publish to Kafka with callback
            self._producer.produce(callback=delivery_callback, **message)
            
            # Poll for events in a non-blocking way
            self._producer.poll(0)
            
            # Wait for the callback to be executed
            return await future
            
        except Exception as e:
            raise GoogleAPICallError(f"Failed to publish message: {e}")

    async def __aenter__(self):
        """Start the Kafka producer client."""
        if self._started:
            return self
            
        try:
            # producer_config = self._create_producer_config()
            self._producer = Producer(self._kafka_config.producer_config)
            self._started = True
            logger.info(f"Kafka producer started for topic: {self._topic_name}")
            return self
        except Exception as e:
            raise GoogleAPICallError(f"Failed to start Kafka producer: {e}")

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the Kafka producer client and flush pending messages."""
        if not self._started:
            return
            
        try:
            if self._producer:
                # Flush any pending messages (wait up to 30 seconds)
                remaining = self._producer.flush(30)
                if remaining is not None and remaining > 0:
                    logger.warning(f"Failed to flush {remaining} messages on shutdown")
                self._producer = None
            self._started = False
            logger.info(f"Kafka producer stopped for topic: {self._topic_name}")
        except Exception as e:
            logger.error(f"Error stopping Kafka producer: {e}")