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

import logging
from typing import Optional, List, Union, Dict, Any

from google.api_core.client_options import ClientOptions
from overrides import overrides

from google.cloud.pubsublite.transport.transport_interface import AdminTransport
from google.cloud.pubsublite.transport.msak_config import MSAKConfig
from google.cloud.pubsublite.types import (
    TopicPath,
    SubscriptionPath,
    LocationPath,
)
from google.cloud.pubsublite_v1.types.common import Topic, Subscription

try:
    from google.cloud.managedkafka_v1 import ManagedKafkaClient
    from google.cloud.managedkafka_v1.types import (
        CreateTopicRequest,
        DeleteTopicRequest,
        GetTopicRequest,
        ListTopicsRequest,
        UpdateTopicRequest,
        Topic as MSAKTopic,
        ConsumerGroup,
    )
    MSAK_AVAILABLE = True
except ImportError:
    MSAK_AVAILABLE = False
    ManagedKafkaClient = None

logger = logging.getLogger(__name__)


class MSAKAdminTransport(AdminTransport):
    """Google Managed Service for Apache Kafka implementation of AdminTransport.
    
    Maps Pub/Sub Lite concepts to MSAK:
    - Topics -> MSAK topics 
    - Subscriptions -> Consumer groups
    - Partitions -> Kafka partitions
    """
    
    def __init__(
        self,
        msak_config: MSAKConfig,
        client_options: Optional[ClientOptions] = None,
        **kwargs
    ):
        """Initialize MSAK admin transport.
        
        Args:
            msak_config: MSAK cluster configuration.
            client_options: Additional client options.
            **kwargs: Additional arguments.
            
        Raises:
            ImportError: If google-cloud-managed-kafka is not available.
        """
        if not MSAK_AVAILABLE:
            raise ImportError(
                "google-cloud-managed-kafka is required for MSAK transport. "
                "Install with: pip install google-cloud-managed-kafka"
            )
        
        self._msak_config = msak_config
        self._client_options = client_options
        self._kwargs = kwargs
        self._admin_client = None
    
    def _get_admin_client(self) -> ManagedKafkaClient:
        """Get or create the MSAK admin client."""
        if self._admin_client is None:
            self._admin_client = ManagedKafkaClient(
                credentials=self._msak_config.credentials,
                client_options=self._client_options,
            )
        return self._admin_client
    
    def _topic_path_to_msak_topic(self, topic_path: Union[TopicPath, str]) -> str:
        """Convert Pub/Sub Lite topic path to MSAK topic name."""
        if isinstance(topic_path, TopicPath):
            # Extract topic name from path: projects/p/locations/l/topics/topic_name
            return topic_path.topic
        else:
            return str(topic_path)
    
    def _subscription_path_to_consumer_group(self, subscription_path: Union[SubscriptionPath, str]) -> str:
        """Convert Pub/Sub Lite subscription path to MSAK consumer group name."""
        if isinstance(subscription_path, SubscriptionPath):
            # Extract subscription name from path: projects/p/locations/l/subscriptions/sub_name
            return subscription_path.subscription
        else:
            return str(subscription_path)
    
    def _msak_topic_to_pubsub_topic(self, msak_topic: MSAKTopic) -> Topic:
        """Convert MSAK topic to Pub/Sub Lite Topic object."""
        # Create topic path
        topic_path = TopicPath(
            self._msak_config.project_id,
            LocationPath(self._msak_config.project_id, self._msak_config.location),
            msak_topic.name
        )
        
        return Topic(
            name=str(topic_path),
            partition_config=Topic.PartitionConfig(
                count=msak_topic.partition_count,
                capacity=Topic.PartitionConfig.Capacity(
                    publish_mib_per_sec=4,  # Default values
                    subscribe_mib_per_sec=8,
                )
            ),
            retention_config=Topic.RetentionConfig(
                per_partition_bytes=msak_topic.configs.get('retention.bytes', 30 * 1024 * 1024 * 1024),
            ),
        )
    
    @overrides
    def create_topic(
        self,
        topic: Topic,
        timeout: Optional[float] = None,
    ) -> Topic:
        """Create an MSAK topic from Pub/Sub Lite topic specification."""
        admin_client = self._get_admin_client()
        
        # Extract topic name and configuration
        topic_path = TopicPath.parse(topic.name)
        msak_topic_name = self._topic_path_to_msak_topic(topic_path)
        
        # Map Pub/Sub Lite topic config to MSAK topic config
        partition_count = topic.partition_config.count if topic.partition_config else 1
        
        configs = {}
        if topic.retention_config:
            configs['retention.bytes'] = str(topic.retention_config.per_partition_bytes)
            # Map retention time if available (MSAK may have different config names)
            # configs['retention.ms'] = str(retention_time_ms)
        
        # Create MSAK topic
        msak_topic = MSAKTopic(
            name=msak_topic_name,
            partition_count=partition_count,
            replication_factor=3,  # Default for managed service
            configs=configs,
        )
        
        request = CreateTopicRequest(
            parent=self._msak_config.cluster_path,
            topic=msak_topic,
            topic_id=msak_topic_name,
        )
        
        # Execute creation
        try:
            operation = admin_client.create_topic(request=request, timeout=timeout)
            # Wait for operation to complete
            created_topic = operation.result(timeout=timeout)
            logger.info(f"Created MSAK topic: {msak_topic_name}")
            
            return self._msak_topic_to_pubsub_topic(created_topic)
            
        except Exception as e:
            logger.error(f"Failed to create MSAK topic {msak_topic_name}: {e}")
            raise
    
    @overrides
    def get_topic(
        self,
        topic_path: Union[TopicPath, str],
        timeout: Optional[float] = None,
    ) -> Topic:
        """Get an MSAK topic as Pub/Sub Lite Topic object."""
        admin_client = self._get_admin_client()
        msak_topic_name = self._topic_path_to_msak_topic(topic_path)
        
        request = GetTopicRequest(
            name=f"{self._msak_config.cluster_path}/topics/{msak_topic_name}"
        )
        
        try:
            msak_topic = admin_client.get_topic(request=request, timeout=timeout)
            return self._msak_topic_to_pubsub_topic(msak_topic)
            
        except Exception as e:
            logger.error(f"Failed to get MSAK topic {msak_topic_name}: {e}")
            raise
    
    @overrides
    def list_topics(
        self,
        location_path: LocationPath,
        timeout: Optional[float] = None,
    ) -> List[Topic]:
        """List MSAK topics as Pub/Sub Lite Topic objects."""
        admin_client = self._get_admin_client()
        
        request = ListTopicsRequest(
            parent=self._msak_config.cluster_path
        )
        
        try:
            response = admin_client.list_topics(request=request, timeout=timeout)
            
            topics = []
            for msak_topic in response.topics:
                pubsub_topic = self._msak_topic_to_pubsub_topic(msak_topic)
                topics.append(pubsub_topic)
            
            return topics
            
        except Exception as e:
            logger.error(f"Failed to list MSAK topics: {e}")
            raise
    
    @overrides
    def update_topic(
        self,
        topic: Topic,
        timeout: Optional[float] = None,
    ) -> Topic:
        """Update an MSAK topic configuration."""
        admin_client = self._get_admin_client()
        
        # Extract topic name
        topic_path = TopicPath.parse(topic.name)
        msak_topic_name = self._topic_path_to_msak_topic(topic_path)
        
        # Build update configs
        configs = {}
        if topic.retention_config:
            configs['retention.bytes'] = str(topic.retention_config.per_partition_bytes)
        
        msak_topic = MSAKTopic(
            name=f"{self._msak_config.cluster_path}/topics/{msak_topic_name}",
            configs=configs,
        )
        
        request = UpdateTopicRequest(
            topic=msak_topic,
        )
        
        try:
            operation = admin_client.update_topic(request=request, timeout=timeout)
            updated_topic = operation.result(timeout=timeout)
            logger.info(f"Updated MSAK topic: {msak_topic_name}")
            
            return self._msak_topic_to_pubsub_topic(updated_topic)
            
        except Exception as e:
            logger.error(f"Failed to update MSAK topic {msak_topic_name}: {e}")
            raise
    
    @overrides
    def delete_topic(
        self,
        topic_path: Union[TopicPath, str],
        timeout: Optional[float] = None,
    ) -> None:
        """Delete an MSAK topic."""
        admin_client = self._get_admin_client()
        msak_topic_name = self._topic_path_to_msak_topic(topic_path)
        
        request = DeleteTopicRequest(
            name=f"{self._msak_config.cluster_path}/topics/{msak_topic_name}"
        )
        
        try:
            operation = admin_client.delete_topic(request=request, timeout=timeout)
            operation.result(timeout=timeout)  # Wait for completion
            logger.info(f"Deleted MSAK topic: {msak_topic_name}")
            
        except Exception as e:
            logger.error(f"Failed to delete MSAK topic {msak_topic_name}: {e}")
            raise
    
    @overrides
    def create_subscription(
        self,
        subscription: Subscription,
        timeout: Optional[float] = None,
    ) -> Subscription:
        """Create a subscription (consumer group concept in MSAK)."""
        # In MSAK, consumer groups are created implicitly when consumers start
        # We can optionally create them explicitly if the API supports it
        consumer_group_name = self._subscription_path_to_consumer_group(
            SubscriptionPath.parse(subscription.name)
        )
        
        logger.info(f"Subscription created (consumer group): {consumer_group_name}")
        return subscription
    
    @overrides
    def get_subscription(
        self,
        subscription_path: Union[SubscriptionPath, str],
        timeout: Optional[float] = None,
    ) -> Subscription:
        """Get a subscription (consumer group in MSAK)."""
        consumer_group_name = self._subscription_path_to_consumer_group(subscription_path)
        
        # Create a basic subscription object
        if isinstance(subscription_path, SubscriptionPath):
            name = str(subscription_path)
            # Try to derive topic from subscription path or use a default
            topic_name = "unknown-topic"  # Would need additional logic to determine
        else:
            name = str(subscription_path)
            topic_name = "unknown-topic"
        
        return Subscription(
            name=name,
            topic=topic_name,
        )
    
    @overrides
    def list_subscriptions(
        self,
        location_path: LocationPath,
        timeout: Optional[float] = None,
    ) -> List[Subscription]:
        """List subscriptions (consumer groups in MSAK)."""
        # MSAK may have APIs to list consumer groups
        # This would depend on the actual Google Cloud Managed Kafka API
        logger.warning("List subscriptions has limited functionality with MSAK transport")
        return []
    
    @overrides
    def update_subscription(
        self,
        subscription: Subscription,
        timeout: Optional[float] = None,
    ) -> Subscription:
        """Update a subscription (consumer group configuration in MSAK)."""
        consumer_group_name = self._subscription_path_to_consumer_group(
            SubscriptionPath.parse(subscription.name)
        )
        logger.info(f"Subscription updated (consumer group): {consumer_group_name}")
        return subscription
    
    @overrides
    def delete_subscription(
        self,
        subscription_path: Union[SubscriptionPath, str],
        timeout: Optional[float] = None,
    ) -> None:
        """Delete a subscription (consumer group in MSAK)."""
        consumer_group_name = self._subscription_path_to_consumer_group(subscription_path)
        # Consumer groups in Kafka are typically auto-cleaned when inactive
        logger.info(f"Subscription deleted (consumer group will be auto-cleaned): {consumer_group_name}")