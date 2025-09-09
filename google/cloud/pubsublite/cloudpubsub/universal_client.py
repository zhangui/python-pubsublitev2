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

import os
from concurrent.futures import Future
from typing import Optional, Mapping, Union, List, Dict, Any

from google.api_core.client_options import ClientOptions
from google.auth.credentials import Credentials
from google.cloud.pubsub_v1.types import BatchSettings

from google.cloud.pubsublite.cloudpubsub.publisher_client import PublisherClient
from google.cloud.pubsublite.cloudpubsub.publisher_client_interface import (
    PublisherClientInterface,
)
from google.cloud.pubsublite.internal.constructable_from_service_account import (
    ConstructableFromServiceAccount,
)
from google.cloud.pubsublite.types import TopicPath
from overrides import overrides

# Optional Kafka imports - only loaded if Kafka functionality is requested
try:
    from google.cloud.pubsublite.cloudpubsub.msak_client import MsakClient, KafkaConfig
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    MsakClient = None
    KafkaConfig = None


class UniversalPublisherClient(PublisherClientInterface, ConstructableFromServiceAccount):
    """
    A universal publisher client that can switch between Pub/Sub Lite and 
    Google Managed Service for Apache Kafka backends.
    
    The backend is chosen based on configuration or environment variables.
    This maintains full compatibility with the existing PublisherClientInterface.
    
    Must be used in a `with` block or have __enter__() called before use.
    """

    def __init__(
        self,
        *,
        # Pub/Sub Lite parameters
        per_partition_batching_settings: Optional[BatchSettings] = None,
        credentials: Optional[Credentials] = None,
        transport: str = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        enable_idempotence: bool = False,
        # Universal client parameters
        use_kafka: Optional[bool] = None,
        kafka_config: Optional[KafkaConfig] = None,
    ):
        """
        Create a new UniversalPublisherClient.

        Args:
            per_partition_batching_settings: The settings for publish batching (Pub/Sub Lite only).
            credentials: If provided, the credentials to use when connecting.
            transport: The transport to use (Pub/Sub Lite only).
            client_options: The client options to use when connecting (Pub/Sub Lite only).
            enable_idempotence: Whether idempotence is enabled (Pub/Sub Lite only).
            use_kafka: If True, use Kafka backend. If False, use Pub/Sub Lite. 
                      If None, check environment variable PUBSUBLITE_USE_KAFKA.
            kafka_config: Configuration for Kafka backend. Required if using Kafka.
        """
        self._use_kafka = self._determine_backend(use_kafka)
        self._backend_client: Optional[PublisherClientInterface] = None
        
        # Store parameters for lazy initialization
        self._pubsublite_params = {
            'per_partition_batching_settings': per_partition_batching_settings,
            'credentials': credentials,
            'transport': transport,
            'client_options': client_options,
            'enable_idempotence': enable_idempotence,
        }
        self._kafka_config = kafka_config

    def _determine_backend(self, use_kafka: Optional[bool]) -> bool:
        """Determine which backend to use based on configuration and environment."""
        if use_kafka is not None:
            return use_kafka
        
        # Check environment variable
        env_var = os.getenv('PUBSUBLITE_USE_KAFKA', '').lower()
        if env_var in ('true', '1', 'yes'):
            return True
        elif env_var in ('false', '0', 'no', ''):
            return False
        else:
            # Default to Pub/Sub Lite for backward compatibility
            return False

    def _create_backend_client(self) -> PublisherClientInterface:
        """Create the appropriate backend client based on configuration."""
        if self._use_kafka:
            if not KAFKA_AVAILABLE:
                raise ImportError(
                    "Kafka functionality requested but confluent-kafka is not installed. "
                    "Install with: pip install google-cloud-pubsublite[kafka]"
                )
            
            if self._kafka_config is None:
                raise ValueError(
                    "kafka_config must be provided when use_kafka=True or "
                    "PUBSUBLITE_USE_KAFKA environment variable is set"
                )
            
            return MsakClient(kafka_config=self._kafka_config)
        else:
            # Use existing Pub/Sub Lite client
            return PublisherClient(**self._pubsublite_params)

    @property
    def backend_type(self) -> str:
        """Return the backend type being used."""
        return "kafka" if self._use_kafka else "pubsublite"

    @overrides
    def publish(
        self,
        topic: Union[TopicPath, str],
        data: bytes,
        ordering_key: str = "",
        **attrs: Mapping[str, str],
    ) -> "Future[str]":
        """
        Publish a message using the configured backend.

        Args:
            topic: The topic to publish to.
            data: The bytestring payload of the message.
            ordering_key: The key to enforce ordering on, or "" for no ordering.
            **attrs: Additional attributes to send.

        Returns:
            A future completed with an ack id. Format depends on backend:
            - Pub/Sub Lite: Encoded message metadata
            - Kafka: topic:partition:offset

        Raises:
            GoogleApiCallError: On a permanent failure.
        """
        if self._backend_client is None:
            future = Future()
            future.set_exception(
                RuntimeError("Client not started. Use with statement or call __enter__()")
            )
            return future

        return self._backend_client.publish(
            topic=topic, data=data, ordering_key=ordering_key, **attrs
        )

    @overrides
    def __enter__(self):
        """Start the backend client."""
        if self._backend_client is None:
            self._backend_client = self._create_backend_client()
        
        self._backend_client.__enter__()
        return self

    @overrides
    def __exit__(self, exc_type, exc_value, traceback):
        """Stop the backend client."""
        if self._backend_client is not None:
            self._backend_client.__exit__(exc_type, exc_value, traceback)


def create_kafka_config(
    bootstrap_servers: List[str],
    auth_endpoint: str = "localhost:14293",
    credentials: Optional[Credentials] = None,
    producer_config: Optional[Dict[str, Any]] = None,
) -> KafkaConfig:
    """
    Convenience function to create a KafkaConfig.
    
    Args:
        bootstrap_servers: List of Kafka bootstrap server addresses.
        auth_endpoint: OAuth authentication endpoint (default: localhost:14293).
        credentials: Google Cloud credentials for authentication.
        producer_config: Additional Kafka producer configuration.
        
    Returns:
        KafkaConfig instance.
        
    Raises:
        ImportError: If Kafka functionality is not available.
    """
    if not KAFKA_AVAILABLE:
        raise ImportError(
            "Kafka functionality not available. Install with: pip install google-cloud-pubsublite[kafka]"
        )
    
    return KafkaConfig(
        bootstrap_servers=bootstrap_servers,
        auth_endpoint=auth_endpoint,
        credentials=credentials,
        producer_config=producer_config or {},
    )