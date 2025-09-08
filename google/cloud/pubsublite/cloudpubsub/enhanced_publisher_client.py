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

from concurrent.futures import Future
from typing import Optional, Mapping, Union

from google.api_core.client_options import ClientOptions
from google.auth.credentials import Credentials
from google.cloud.pubsub_v1.types import BatchSettings, PubsubMessage
from overrides import overrides

from google.cloud.pubsublite.cloudpubsub.publisher_client_interface import (
    PublisherClientInterface,
)
from google.cloud.pubsublite.internal.constructable_from_service_account import (
    ConstructableFromServiceAccount,
)
from google.cloud.pubsublite.internal.require_started import RequireStarted
from google.cloud.pubsublite.types import TopicPath
from google.cloud.pubsublite.transport import (
    TransportType,
    TransportFactory,
    MSAKConfig,
    PublisherTransport,
)


class EnhancedPublisherClient(PublisherClientInterface, ConstructableFromServiceAccount):
    """
    Enhanced PublisherClient that supports both Pub/Sub Lite and Google MSAK.
    
    This client provides a unified interface for publishing messages to either
    Google Cloud Pub/Sub Lite or Google Managed Service for Apache Kafka (MSAK).
    """

    _transport: PublisherTransport
    _require_started: RequireStarted

    def __init__(
        self,
        topic: Union[TopicPath, str],
        *,
        transport_type: TransportType = TransportType.PUBSUB_LITE,
        per_partition_batching_settings: Optional[BatchSettings] = None,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
        # MSAK-specific parameters
        msak_config: Optional[MSAKConfig] = None,
    ):
        """
        Create a new EnhancedPublisherClient.

        Args:
            topic: Topic to publish to. For Pub/Sub Lite, use TopicPath. For MSAK, use string topic name.
            transport_type: The transport backend to use (PUBSUB_LITE or MANAGED_KAFKA).
            per_partition_batching_settings: The settings for publish batching.
            credentials: Google Cloud credentials to use when connecting.
            client_options: The client options to use when connecting.
            msak_config: MSAK cluster configuration (required for MANAGED_KAFKA transport).
        
        Raises:
            ValueError: If required configuration is missing for the selected transport.
        """
        self._topic = topic
        self._transport_type = transport_type
        self._require_started = RequireStarted()
        
        # Create the appropriate transport
        self._transport = TransportFactory.create_publisher_transport(
            transport_type=transport_type,
            topic=topic,
            batch_settings=per_partition_batching_settings,
            credentials=credentials,
            client_options=client_options,
            msak_config=msak_config,
        )

    @overrides
    def publish(
        self,
        topic: Union[TopicPath, str],
        data: bytes,
        ordering_key: str = "",
        **attrs: Mapping[str, str],
    ) -> "Future[str]":
        """Publish a message.
        
        Args:
            topic: Topic to publish to (must match the client's topic).
            data: The message payload.
            ordering_key: Optional ordering key.
            **attrs: Message attributes.
            
        Returns:
            Future that resolves to the message ID.
            
        Raises:
            ValueError: If topic doesn't match the client's topic.
        """
        self._require_started.require_started()
        
        # Validate topic matches client configuration
        if str(topic) != str(self._topic):
            raise ValueError(f"Topic {topic} doesn't match client topic {self._topic}")
        
        # Create PubsubMessage
        message = PubsubMessage(
            data=data,
            attributes=dict(attrs),
            ordering_key=ordering_key,
        )
        
        # Use transport to publish
        return self._transport.publish(message, ordering_key or None)
    
    @overrides
    def __enter__(self):
        """Enter context manager."""
        self._require_started.__enter__()
        self._transport.__enter__()
        return self

    @overrides
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        try:
            self._transport.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._require_started.__exit__(exc_type, exc_val, exc_tb)

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush pending messages.
        
        Args:
            timeout: Maximum time to wait for flush in seconds.
        """
        if hasattr(self._transport, 'flush'):
            self._transport.flush(timeout)

    @property
    def transport_type(self) -> TransportType:
        """Get the transport type being used."""
        return self._transport_type
    
    @classmethod
    def create_for_pubsub_lite(
        cls,
        topic: TopicPath,
        *,
        per_partition_batching_settings: Optional[BatchSettings] = None,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
    ) -> "EnhancedPublisherClient":
        """Create a client for Pub/Sub Lite.
        
        Args:
            topic: Pub/Sub Lite topic path.
            per_partition_batching_settings: Batching settings.
            credentials: Google Cloud credentials.
            client_options: Client options.
            
        Returns:
            EnhancedPublisherClient configured for Pub/Sub Lite.
        """
        return cls(
            topic=topic,
            transport_type=TransportType.PUBSUB_LITE,
            per_partition_batching_settings=per_partition_batching_settings,
            credentials=credentials,
            client_options=client_options,
        )
    
    @classmethod
    def create_for_msak(
        cls,
        topic: str,
        msak_config: MSAKConfig,
        *,
        per_partition_batching_settings: Optional[BatchSettings] = None,
        client_options: Optional[ClientOptions] = None,
    ) -> "EnhancedPublisherClient":
        """Create a client for Google MSAK.
        
        Args:
            topic: Kafka topic name.
            msak_config: MSAK cluster configuration.
            per_partition_batching_settings: Batching settings.
            client_options: Client options.
            
        Returns:
            EnhancedPublisherClient configured for MSAK.
        """
        return cls(
            topic=topic,
            transport_type=TransportType.MANAGED_KAFKA,
            per_partition_batching_settings=per_partition_batching_settings,
            credentials=msak_config.credentials,
            client_options=client_options,
            msak_config=msak_config,
        )