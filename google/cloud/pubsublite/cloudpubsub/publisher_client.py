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

import os
from concurrent.futures import Future
from typing import Optional, Mapping, Union, Dict, Any
from uuid import uuid4

from google.api_core.client_options import ClientOptions
from google.auth.credentials import Credentials
from google.cloud.pubsub_v1.types import BatchSettings

from google.cloud.pubsublite.cloudpubsub.internal.make_publisher import (
    make_publisher,
    make_async_publisher,
)
from google.cloud.pubsublite.cloudpubsub.internal.multiplexed_async_publisher_client import (
    MultiplexedAsyncPublisherClient,
)
from google.cloud.pubsublite.cloudpubsub.internal.multiplexed_publisher_client import (
    MultiplexedPublisherClient,
)
from google.cloud.pubsublite.cloudpubsub.publisher_client_interface import (
    PublisherClientInterface,
    AsyncPublisherClientInterface,
)
from google.cloud.pubsublite.internal.constructable_from_service_account import (
    ConstructableFromServiceAccount,
)
from google.cloud.pubsublite.internal.publisher_client_id import PublisherClientId
from google.cloud.pubsublite.internal.require_started import RequireStarted
from google.cloud.pubsublite.internal.wire.make_publisher import (
    DEFAULT_BATCHING_SETTINGS as WIRE_DEFAULT_BATCHING,
)
from google.cloud.pubsublite.types import TopicPath
from overrides import overrides

# Optional Kafka imports - only loaded if Kafka functionality is requested
try:
    from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import (
        KafkaConfig,
        KafkaConfigBuilder,
        create_oauth_token_callback,
    )
    KAFKA_AVAILABLE = True
except ImportError:
    KafkaConfig = None
    KafkaConfigBuilder = None
    create_oauth_token_callback = None
    KAFKA_AVAILABLE = False


def _get_client_id(enable_idempotence: bool):
    return PublisherClientId(uuid4().bytes) if enable_idempotence else None


def _determine_backend(use_kafka: Optional[bool]) -> bool:
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


def _create_kafka_config(
    credentials: Optional[Credentials] = None,
    producer_config: Optional[Dict[str, Any]] = None,
    batch_settings: Optional[BatchSettings] = None,
) -> KafkaConfig:
    """
    Create a KafkaConfig with the provided parameters.
    
    Args:
        credentials: Google Cloud credentials for OAuth authentication
        producer_config: Complete Kafka producer configuration
        batch_settings: Optional Pub/Sub Lite batch settings to convert
    
    Returns:
        KafkaConfig instance
    """
    if not KAFKA_AVAILABLE:
        raise ImportError(
            "Kafka functionality requested but confluent-kafka is not installed. "
            "Install with: pip install google-cloud-pubsublite[kafka]"
        )
    
    # Require explicit configuration
    if not producer_config:
        raise ValueError(
            "kafka_producer_config is required when using Kafka backend. "
            "Please provide a complete Kafka producer configuration including "
            "'bootstrap.servers' and authentication settings."
        )
    
    # Validate required fields
    if 'bootstrap.servers' not in producer_config:
        raise ValueError(
            "kafka_producer_config must contain 'bootstrap.servers'"
        )
    
    # Validate authentication configuration
    security_protocol = producer_config.get('security.protocol')
    if security_protocol == 'SASL_SSL':
        # Validate OAuth/SASL configuration
        if 'sasl.mechanisms' not in producer_config:
            raise ValueError(
                "SASL_SSL requires 'sasl.mechanisms' to be specified"
            )
        if producer_config.get('sasl.mechanisms') == 'OAUTHBEARER':
            if 'oauth_cb' not in producer_config:
                # If no oauth_cb provided, create one using credentials
                if credentials:
                    producer_config['oauth_cb'] = create_oauth_token_callback(credentials)
                else:
                    raise ValueError(
                        "OAUTHBEARER requires 'oauth_cb' token provider or credentials"
                    )
    elif security_protocol == 'SSL':
        # Validate mTLS configuration
        required_ssl_fields = [
            'ssl.certificate.location',
            'ssl.key.location', 
            'ssl.ca.location'
        ]
        missing_fields = [f for f in required_ssl_fields if f not in producer_config]
        if missing_fields:
            raise ValueError(
                f"mTLS configuration requires: {', '.join(missing_fields)}"
            )
    elif security_protocol is None:
        raise ValueError(
            "kafka_producer_config must specify 'security.protocol' "
            "(e.g., 'SASL_SSL' for OAuth or 'SSL' for mTLS)"
        )
    
    # Apply batch settings if provided
    if batch_settings and KafkaConfigBuilder:
        batch_config = KafkaConfigBuilder.from_batch_settings(batch_settings)
        # Merge batch settings into producer config (user config takes precedence)
        for key, value in batch_config.items():
            if key not in producer_config:
                producer_config[key] = value
    
    return KafkaConfig(
        credentials=credentials,
        producer_config=producer_config,
    )


class PublisherClient(PublisherClientInterface, ConstructableFromServiceAccount):
    """
    A PublisherClient publishes messages similar to Google Pub/Sub.
    Any publish failures are unlikely to succeed if retried.

    Can publish to either Pub/Sub Lite or Google Managed Service for Apache Kafka
    based on configuration.

    Must be used in a `with` block or have __enter__() called before use.
    """

    _impl: PublisherClientInterface
    _require_started: RequireStarted

    DEFAULT_BATCHING_SETTINGS = WIRE_DEFAULT_BATCHING
    """
    The default batching settings for a publisher client.
    """

    def __init__(
        self,
        *,
        per_partition_batching_settings: Optional[BatchSettings] = None,
        credentials: Optional[Credentials] = None,
        transport: str = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        enable_idempotence: bool = False,
        # Kafka-specific parameters
        use_kafka: Optional[bool] = None,
        kafka_producer_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Create a new PublisherClient.

        Args:
            per_partition_batching_settings: The settings for publish batching. Apply on a per-partition basis.
            credentials: If provided, the credentials to use when connecting.
            transport: The transport to use. Must correspond to an asyncio transport.
            client_options: The client options to use when connecting. If used, must explicitly set `api_endpoint`.
            enable_idempotence: Whether idempotence is enabled, where the server will ensure that unique messages within a single publisher session are stored only once.
            use_kafka: If True, use Kafka backend. If False, use Pub/Sub Lite. If None, check environment variable PUBSUBLITE_USE_KAFKA.
            kafka_producer_config: Kafka producer configuration options. Must contain 'bootstrap.servers' when using Kafka backend.
        """
        # Determine which backend to use
        actual_use_kafka = _determine_backend(use_kafka)
        
        # Create Kafka config if using Kafka backend
        kafka_config = None
        if actual_use_kafka:
            kafka_config = _create_kafka_config(
                credentials=credentials,
                producer_config=kafka_producer_config,
                batch_settings=per_partition_batching_settings,
            )
        
        # Create implementation using the factory pattern
        client_id = _get_client_id(enable_idempotence)
        self._impl = MultiplexedPublisherClient(
            lambda topic: make_publisher(
                topic=topic,
                per_partition_batching_settings=per_partition_batching_settings,
                credentials=credentials,
                client_options=client_options,
                transport=transport,
                client_id=client_id,
                use_kafka=actual_use_kafka,
                kafka_config=kafka_config,
            )
        )
        self._require_started = RequireStarted()

    @overrides
    def publish(
        self,
        topic: Union[TopicPath, str],
        data: bytes,
        ordering_key: str = "",
        **attrs: Mapping[str, str],
    ) -> "Future[str]":
        self._require_started.require_started()
        return self._impl.publish(
            topic=topic, data=data, ordering_key=ordering_key, **attrs
        )

    @overrides
    def __enter__(self):
        self._require_started.__enter__()
        self._impl.__enter__()
        return self

    @overrides
    def __exit__(self, exc_type, exc_value, traceback):
        self._impl.__exit__(exc_type, exc_value, traceback)
        self._require_started.__exit__(exc_type, exc_value, traceback)


class AsyncPublisherClient(
    AsyncPublisherClientInterface, ConstructableFromServiceAccount
):
    """
    An AsyncPublisherClient publishes messages similar to Google Pub/Sub, but must be used in an
    async context. Any publish failures are unlikely to succeed if retried.

    Can publish to either Pub/Sub Lite or Google Managed Service for Apache Kafka
    based on configuration.

    Must be used in an `async with` block or have __aenter__() awaited before use.
    """

    _impl: AsyncPublisherClientInterface
    _require_started: RequireStarted

    DEFAULT_BATCHING_SETTINGS = WIRE_DEFAULT_BATCHING
    """
    The default batching settings for a publisher client.
    """

    def __init__(
        self,
        *,
        per_partition_batching_settings: Optional[BatchSettings] = None,
        credentials: Optional[Credentials] = None,
        transport: str = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        enable_idempotence: bool = False,
        # Kafka-specific parameters
        use_kafka: Optional[bool] = None,
        kafka_producer_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Create a new AsyncPublisherClient.

        Args:
            per_partition_batching_settings: The settings for publish batching. Apply on a per-partition basis.
            credentials: If provided, the credentials to use when connecting.
            transport: The transport to use. Must correspond to an asyncio transport.
            client_options: The client options to use when connecting. If used, must explicitly set `api_endpoint`.
            enable_idempotence: Whether idempotence is enabled, where the server will ensure that unique messages within a single publisher session are stored only once.
            use_kafka: If True, use Kafka backend. If False, use Pub/Sub Lite. If None, check environment variable PUBSUBLITE_USE_KAFKA.
            kafka_producer_config: Kafka producer configuration options. Must contain 'bootstrap.servers' when using Kafka backend.
        """
        # Determine which backend to use
        actual_use_kafka = _determine_backend(use_kafka)
        
        # Create Kafka config if using Kafka backend
        kafka_config = None
        if actual_use_kafka:
            kafka_config = _create_kafka_config(
                credentials=credentials,
                producer_config=kafka_producer_config,
                batch_settings=per_partition_batching_settings,
            )

        client_id = _get_client_id(enable_idempotence)
        self._impl = MultiplexedAsyncPublisherClient(
            lambda topic: make_async_publisher(
                topic=topic,
                per_partition_batching_settings=per_partition_batching_settings,
                credentials=credentials,
                client_options=client_options,
                transport=transport,
                client_id=client_id,
                use_kafka=actual_use_kafka,
                kafka_config=kafka_config,
            )
        )
        self._require_started = RequireStarted()

    @overrides
    async def publish(
        self,
        topic: Union[TopicPath, str],
        data: bytes,
        ordering_key: str = "",
        **attrs: Mapping[str, str],
    ) -> str:
        self._require_started.require_started()
        return await self._impl.publish(
            topic=topic, data=data, ordering_key=ordering_key, **attrs
        )

    @overrides
    async def __aenter__(self):
        self._require_started.__enter__()
        await self._impl.__aenter__()
        return self

    @overrides
    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._impl.__aexit__(exc_type, exc_value, traceback)
        self._require_started.__exit__(exc_type, exc_value, traceback)
