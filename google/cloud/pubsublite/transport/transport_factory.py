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

from typing import Union, Optional
from google.api_core.client_options import ClientOptions
from google.auth.credentials import Credentials
from google.cloud.pubsub_v1.types import BatchSettings

from google.cloud.pubsublite.transport.transport_interface import (
    TransportType,
    PublisherTransport,
    AsyncPublisherTransport,
    SubscriberTransport,
    AsyncSubscriberTransport,
    AdminTransport,
)
from google.cloud.pubsublite.transport.msak_config import MSAKConfig
from google.cloud.pubsublite.types import (
    TopicPath,
    SubscriptionPath,
    LocationPath,
    FlowControlSettings,
)


class TransportFactory:
    """Factory for creating transport implementations."""
    
    @staticmethod
    def create_publisher_transport(
        transport_type: TransportType,
        topic: Union[TopicPath, str],
        batch_settings: Optional[BatchSettings] = None,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
        msak_config: Optional[MSAKConfig] = None,
        **kwargs
    ) -> PublisherTransport:
        """Create a publisher transport.
        
        Args:
            transport_type: Type of transport to create.
            topic: Topic path or name to publish to.
            batch_settings: Batching configuration.
            credentials: Google Cloud credentials (for Pub/Sub Lite).
            client_options: Client options (for Pub/Sub Lite).
            msak_config: MSAK configuration (for Managed Kafka).
            **kwargs: Additional transport-specific arguments.
            
        Returns:
            Publisher transport implementation.
            
        Raises:
            ValueError: If configuration is invalid for the transport type.
        """
        if transport_type == TransportType.MANAGED_KAFKA:
            if msak_config is None:
                raise ValueError("MSAK config required for Managed Kafka transport")
            
            from google.cloud.pubsublite.transport.msak_publisher_transport import (
                MSAKPublisherTransport
            )
            return MSAKPublisherTransport(
                topic=str(topic),
                msak_config=msak_config,
                batch_settings=batch_settings,
                client_options=client_options,
                **kwargs
            )
        
        elif transport_type == TransportType.PUBSUB_LITE:
            if not isinstance(topic, TopicPath):
                raise ValueError("TopicPath required for Pub/Sub Lite transport")
            
            from google.cloud.pubsublite.transport.pubsublite_publisher_transport import (
                PubSubLitePublisherTransport
            )
            return PubSubLitePublisherTransport(
                topic=topic,
                batch_settings=batch_settings,
                credentials=credentials,
                client_options=client_options,
                **kwargs
            )
        
        else:
            raise ValueError(f"Unsupported transport type: {transport_type}")
    
    @staticmethod
    def create_async_publisher_transport(
        transport_type: TransportType,
        topic: Union[TopicPath, str],
        batch_settings: Optional[BatchSettings] = None,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
        msak_config: Optional[MSAKConfig] = None,
        **kwargs
    ) -> AsyncPublisherTransport:
        """Create an async publisher transport.
        
        Args:
            transport_type: Type of transport to create.
            topic: Topic path or name to publish to.
            batch_settings: Batching configuration.
            credentials: Google Cloud credentials (for Pub/Sub Lite).
            client_options: Client options (for Pub/Sub Lite).
            msak_config: MSAK configuration (for Managed Kafka).
            **kwargs: Additional transport-specific arguments.
            
        Returns:
            Async publisher transport implementation.
            
        Raises:
            ValueError: If configuration is invalid for the transport type.
        """
        if transport_type == TransportType.MANAGED_KAFKA:
            if msak_config is None:
                raise ValueError("MSAK config required for Managed Kafka transport")
            
            # For now, MSAK async publisher uses the same implementation
            # In a full implementation, you might have a separate async version
            from google.cloud.pubsublite.transport.msak_publisher_transport import (
                MSAKPublisherTransport
            )
            return MSAKPublisherTransport(
                topic=str(topic),
                msak_config=msak_config,
                batch_settings=batch_settings,
                client_options=client_options,
                **kwargs
            )
        
        elif transport_type == TransportType.PUBSUB_LITE:
            if not isinstance(topic, TopicPath):
                raise ValueError("TopicPath required for Pub/Sub Lite transport")
            
            from google.cloud.pubsublite.transport.pubsublite_async_publisher_transport import (
                PubSubLiteAsyncPublisherTransport
            )
            return PubSubLiteAsyncPublisherTransport(
                topic=topic,
                batch_settings=batch_settings,
                credentials=credentials,
                client_options=client_options,
                **kwargs
            )
        
        else:
            raise ValueError(f"Unsupported transport type: {transport_type}")
    
    @staticmethod
    def create_subscriber_transport(
        transport_type: TransportType,
        subscription: Union[SubscriptionPath, str],
        flow_control_settings: Optional[FlowControlSettings] = None,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
        msak_config: Optional[MSAKConfig] = None,
        **kwargs
    ) -> SubscriberTransport:
        """Create a subscriber transport.
        
        Args:
            transport_type: Type of transport to create.
            subscription: Subscription path or name to subscribe to.
            flow_control_settings: Flow control configuration.
            credentials: Google Cloud credentials (for Pub/Sub Lite).
            client_options: Client options (for Pub/Sub Lite).
            msak_config: MSAK configuration (for Managed Kafka).
            **kwargs: Additional transport-specific arguments.
            
        Returns:
            Subscriber transport implementation.
            
        Raises:
            ValueError: If configuration is invalid for the transport type.
        """
        if transport_type == TransportType.MANAGED_KAFKA:
            if msak_config is None:
                raise ValueError("MSAK config required for Managed Kafka transport")
            
            from google.cloud.pubsublite.transport.msak_subscriber_transport import (
                MSAKSubscriberTransport
            )
            
            # For MSAK, we need both subscription (consumer group) and topic
            # Extract topic from subscription path or use a default mapping
            if isinstance(subscription, SubscriptionPath):
                # Simple mapping: subscription name maps to topic name
                topic_name = subscription.subscription
            else:
                # If it's a string, assume format: topic_subscription or just use as topic
                topic_name = str(subscription).replace('_subscription', '')
            
            return MSAKSubscriberTransport(
                subscription=str(subscription),  # Consumer group name
                topic=topic_name,  # Kafka topic name
                msak_config=msak_config,
                flow_control_settings=flow_control_settings,
                client_options=client_options,
                **kwargs
            )
        
        elif transport_type == TransportType.PUBSUB_LITE:
            if not isinstance(subscription, SubscriptionPath):
                raise ValueError("SubscriptionPath required for Pub/Sub Lite transport")
            
            from google.cloud.pubsublite.transport.pubsublite_subscriber_transport import (
                PubSubLiteSubscriberTransport
            )
            return PubSubLiteSubscriberTransport(
                subscription=subscription,
                flow_control_settings=flow_control_settings,
                credentials=credentials,
                client_options=client_options,
                **kwargs
            )
        
        else:
            raise ValueError(f"Unsupported transport type: {transport_type}")
    
    @staticmethod
    def create_async_subscriber_transport(
        transport_type: TransportType,
        subscription: Union[SubscriptionPath, str],
        flow_control_settings: Optional[FlowControlSettings] = None,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
        msak_config: Optional[MSAKConfig] = None,
        **kwargs
    ) -> AsyncSubscriberTransport:
        """Create an async subscriber transport.
        
        Args:
            transport_type: Type of transport to create.
            subscription: Subscription path or name to subscribe to.
            flow_control_settings: Flow control configuration.
            credentials: Google Cloud credentials (for Pub/Sub Lite).
            client_options: Client options (for Pub/Sub Lite).
            msak_config: MSAK configuration (for Managed Kafka).
            **kwargs: Additional transport-specific arguments.
            
        Returns:
            Async subscriber transport implementation.
            
        Raises:
            ValueError: If configuration is invalid for the transport type.
        """
        if transport_type == TransportType.MANAGED_KAFKA:
            if msak_config is None:
                raise ValueError("MSAK config required for Managed Kafka transport")
            
            # For now, use the same MSAK subscriber implementation
            from google.cloud.pubsublite.transport.msak_subscriber_transport import (
                MSAKSubscriberTransport
            )
            
            # Extract topic from subscription
            if isinstance(subscription, SubscriptionPath):
                topic_name = subscription.subscription
            else:
                topic_name = str(subscription).replace('_subscription', '')
            
            return MSAKSubscriberTransport(
                subscription=str(subscription),
                topic=topic_name,
                msak_config=msak_config,
                flow_control_settings=flow_control_settings,
                client_options=client_options,
                **kwargs
            )
        
        elif transport_type == TransportType.PUBSUB_LITE:
            if not isinstance(subscription, SubscriptionPath):
                raise ValueError("SubscriptionPath required for Pub/Sub Lite transport")
            
            from google.cloud.pubsublite.transport.pubsublite_async_subscriber_transport import (
                PubSubLiteAsyncSubscriberTransport
            )
            return PubSubLiteAsyncSubscriberTransport(
                subscription=subscription,
                flow_control_settings=flow_control_settings,
                credentials=credentials,
                client_options=client_options,
                **kwargs
            )
        
        else:
            raise ValueError(f"Unsupported transport type: {transport_type}")
    
    @staticmethod
    def create_admin_transport(
        transport_type: TransportType,
        region: str,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
        msak_config: Optional[MSAKConfig] = None,
        **kwargs
    ) -> AdminTransport:
        """Create an admin transport.
        
        Args:
            transport_type: Type of transport to create.
            region: Region for the transport.
            credentials: Google Cloud credentials (for Pub/Sub Lite).
            client_options: Client options (for Pub/Sub Lite).
            msak_config: MSAK configuration (for Managed Kafka).
            **kwargs: Additional transport-specific arguments.
            
        Returns:
            Admin transport implementation.
            
        Raises:
            ValueError: If configuration is invalid for the transport type.
        """
        if transport_type == TransportType.MANAGED_KAFKA:
            if msak_config is None:
                raise ValueError("MSAK config required for Managed Kafka transport")
            
            from google.cloud.pubsublite.transport.msak_admin_transport import (
                MSAKAdminTransport
            )
            return MSAKAdminTransport(
                msak_config=msak_config,
                client_options=client_options,
                **kwargs
            )
        
        elif transport_type == TransportType.PUBSUB_LITE:
            from google.cloud.pubsublite.transport.pubsublite_admin_transport import (
                PubSubLiteAdminTransport
            )
            return PubSubLiteAdminTransport(
                region=region,
                credentials=credentials,
                client_options=client_options,
                **kwargs
            )
        
        else:
            raise ValueError(f"Unsupported transport type: {transport_type}")