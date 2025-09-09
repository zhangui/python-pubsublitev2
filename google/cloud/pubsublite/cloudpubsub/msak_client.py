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

import json
import logging
import os
from concurrent.futures import Future
from typing import Mapping, Union, Optional, List, Dict, Any

from google.api_core.exceptions import GoogleAPICallError
from google.auth.credentials import Credentials
from google.auth import default

from google.cloud.pubsublite.cloudpubsub.publisher_client_interface import (
    PublisherClientInterface,
)
from google.cloud.pubsublite.types import TopicPath
from overrides import overrides

try:
    from confluent_kafka import Producer, KafkaError
except ImportError:
    raise ImportError(
        "confluent-kafka is required for Kafka functionality. "
        "Install it with: pip install confluent-kafka"
    )

logger = logging.getLogger(__name__)


class KafkaConfig:
    """Configuration for Kafka client connection."""
    
    def __init__(
        self,
        bootstrap_servers: List[str],
        auth_endpoint: str = "localhost:14293",
        credentials: Optional[Credentials] = None,
        producer_config: Optional[Dict[str, Any]] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.auth_endpoint = auth_endpoint
        self.credentials = credentials  # Don't load default credentials in __init__
        self.producer_config = producer_config or {}
    
    def _get_credentials(self) -> Credentials:
        """Get credentials, loading defaults if not provided."""
        if self.credentials is None:
            return default()[0]
        return self.credentials


class MsakClient(PublisherClientInterface):
    """
    A Kafka-based publisher client that implements the PublisherClientInterface
    for Google Managed Service for Apache Kafka (MSAK).
    
    This client publishes messages to Kafka topics using the confluent-kafka library
    while maintaining compatibility with the Pub/Sub Lite client interface.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
    ):
        """
        Create a new MsakClient.

        Args:
            kafka_config: Configuration for Kafka connection including bootstrap servers,
                         authentication, and producer settings.
        """
        self._kafka_config = kafka_config
        self._producer: Optional[Producer] = None
        self._started = False

    def _create_producer_config(self) -> Dict[str, Any]:
        """Create the confluent-kafka Producer configuration."""
        config = {
            'bootstrap.servers': ','.join(self._kafka_config.bootstrap_servers),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'sasl.oauthbearer.token.endpoint.url': self._kafka_config.auth_endpoint,
            'sasl.oauthbearer.client.id': 'unused',
            'sasl.oauthbearer.client.secret': 'unused',
            'sasl.oauthbearer.method': 'oidc',
            # Producer performance settings
            'batch.size': 16384,
            'linger.ms': 5,
            'compression.type': 'snappy',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 100,
        }
        
        # Override with user-provided config
        config.update(self._kafka_config.producer_config)
        return config

    def _topic_path_to_kafka_topic(self, topic: Union[TopicPath, str]) -> str:
        """Convert a TopicPath to a Kafka topic name."""
        if isinstance(topic, str):
            topic = TopicPath.parse(topic)
        
        # Extract just the topic name from the full path
        # Format: projects/{project}/locations/{location}/topics/{topic}
        return topic.name

    def _create_kafka_message(
        self, 
        data: bytes, 
        ordering_key: str = "", 
        **attrs: Mapping[str, str]
    ) -> Dict[str, Any]:
        """Create a Kafka message from Pub/Sub Lite message components."""
        message = {
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

    def _delivery_callback(self, future: Future, err: Optional[KafkaError], msg):
        """Callback for Kafka message delivery."""
        if err:
            # Map Kafka errors to GoogleAPICallError
            error_msg = f"Kafka delivery failed: {err}"
            if err.code() == KafkaError._PARTITION_EOF:
                # This is not actually an error
                future.set_result(str(msg.offset()))
            else:
                future.set_exception(GoogleAPICallError(error_msg))
        else:
            # Success - return message offset as ack ID
            ack_id = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
            future.set_result(ack_id)

    @overrides
    def publish(
        self,
        topic: Union[TopicPath, str],
        data: bytes,
        ordering_key: str = "",
        **attrs: Mapping[str, str],
    ) -> "Future[str]":
        """
        Publish a message to a Kafka topic.

        Args:
            topic: The topic to publish to.
            data: The bytestring payload of the message.
            ordering_key: The key to enforce ordering on, or "" for no ordering.
            **attrs: Additional attributes to send as Kafka headers.

        Returns:
            A future completed with an ack id containing topic:partition:offset.

        Raises:
            GoogleAPICallError: On a permanent failure.
        """
        if not self._started:
            future = Future()
            future.set_exception(
                GoogleAPICallError("Client not started. Use with statement or call __enter__()")
            )
            return future

        try:
            kafka_topic = self._topic_path_to_kafka_topic(topic)
            message = self._create_kafka_message(data, ordering_key, **attrs)
            
            # Create future for async result
            result_future = Future()
            
            # Publish to Kafka with callback
            self._producer.produce(
                topic=kafka_topic,
                callback=lambda err, msg: self._delivery_callback(result_future, err, msg),
                **message
            )
            
            # Trigger any queued callbacks
            self._producer.poll(0)
            
            return result_future
            
        except Exception as e:
            future = Future()
            future.set_exception(GoogleAPICallError(f"Failed to publish message: {e}"))
            return future

    @overrides
    def __enter__(self):
        """Start the Kafka producer client."""
        if self._started:
            return self
            
        try:
            producer_config = self._create_producer_config()
            self._producer = Producer(producer_config)
            self._started = True
            logger.info("Kafka producer client started")
            return self
        except Exception as e:
            raise GoogleAPICallError(f"Failed to start Kafka producer: {e}")

    @overrides
    def __exit__(self, exc_type, exc_value, traceback):
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
            logger.info("Kafka producer client stopped")
        except Exception as e:
            logger.error(f"Error stopping Kafka producer: {e}")