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
        create_default_kafka_producer_config,
    )
    KAFKA_AVAILABLE = True
except ImportError:
    KafkaConfig = None
    create_default_kafka_producer_config = None
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
) -> KafkaConfig:
    """Create a KafkaConfig with the provided parameters."""
    if not KAFKA_AVAILABLE:
        raise ImportError(
            "Kafka functionality requested but confluent-kafka is not installed. "
            "Install with: pip install google-cloud-pubsublite[kafka]"
        )
    
    # Validate that bootstrap.servers is provided
    if not producer_config or 'bootstrap.servers' not in producer_config:
        raise ValueError(
            "kafka_producer_config must contain 'bootstrap.servers' when using Kafka backend"
        )
    
    # Merge default config with user-provided config
    default_config = create_default_kafka_producer_config()
    final_config = default_config.copy()
    final_config.update(producer_config)
    
    return KafkaConfig(
        credentials=credentials,
        producer_config=final_config,
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
