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

import abc
from enum import Enum
from typing import Optional, List, Union, AsyncIterator, Callable
from concurrent.futures import Future

from google.cloud.pubsub_v1.types import PubsubMessage
from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture

from google.cloud.pubsublite_v1.types.common import Topic, Subscription
from google.cloud.pubsublite.types import (
    TopicPath,
    SubscriptionPath,
    LocationPath,
    FlowControlSettings,
)


class TransportType(Enum):
    """Transport backend types."""
    PUBSUB_LITE = "pubsub_lite"
    MANAGED_KAFKA = "msak"  # Google Managed Service for Apache Kafka


class PublisherTransport(abc.ABC):
    """Abstract interface for publisher transport implementations."""
    
    @abc.abstractmethod
    def publish(
        self,
        message: PubsubMessage,
        ordering_key: Optional[str] = None,
    ) -> Future:
        """Publish a message.
        
        Args:
            message: The message to publish.
            ordering_key: Optional ordering key for message ordering.
            
        Returns:
            Future that resolves to the message ID when published.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def __enter__(self):
        """Enter context manager."""
        raise NotImplementedError
    
    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        raise NotImplementedError


class AsyncPublisherTransport(abc.ABC):
    """Abstract interface for async publisher transport implementations."""
    
    @abc.abstractmethod
    async def publish(
        self,
        message: PubsubMessage,
        ordering_key: Optional[str] = None,
    ) -> str:
        """Publish a message asynchronously.
        
        Args:
            message: The message to publish.
            ordering_key: Optional ordering key for message ordering.
            
        Returns:
            The message ID when published.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    async def __aenter__(self):
        """Enter async context manager."""
        raise NotImplementedError
    
    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit async context manager."""
        raise NotImplementedError


class SubscriberTransport(abc.ABC):
    """Abstract interface for subscriber transport implementations."""
    
    @abc.abstractmethod
    def subscribe(
        self,
        callback: Callable[[Message], None],
        flow_control: Optional[FlowControlSettings] = None,
    ) -> StreamingPullFuture:
        """Subscribe to messages.
        
        Args:
            callback: Callback function to handle received messages.
            flow_control: Optional flow control settings.
            
        Returns:
            Future representing the streaming pull operation.
        """
        raise NotImplementedError


class AsyncSubscriberTransport(abc.ABC):
    """Abstract interface for async subscriber transport implementations."""
    
    @abc.abstractmethod
    async def subscribe(
        self,
        flow_control: Optional[FlowControlSettings] = None,
    ) -> AsyncIterator[Message]:
        """Subscribe to messages asynchronously.
        
        Args:
            flow_control: Optional flow control settings.
            
        Returns:
            Async iterator of received messages.
        """
        raise NotImplementedError


class AdminTransport(abc.ABC):
    """Abstract interface for admin transport implementations."""
    
    @abc.abstractmethod
    def create_topic(
        self,
        topic: Topic,
        timeout: Optional[float] = None,
    ) -> Topic:
        """Create a topic.
        
        Args:
            topic: The topic to create.
            timeout: Optional timeout in seconds.
            
        Returns:
            The created topic.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def get_topic(
        self,
        topic_path: Union[TopicPath, str],
        timeout: Optional[float] = None,
    ) -> Topic:
        """Get a topic.
        
        Args:
            topic_path: Path or name of the topic.
            timeout: Optional timeout in seconds.
            
        Returns:
            The topic.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def list_topics(
        self,
        location_path: LocationPath,
        timeout: Optional[float] = None,
    ) -> List[Topic]:
        """List topics in a location.
        
        Args:
            location_path: The location to list topics in.
            timeout: Optional timeout in seconds.
            
        Returns:
            List of topics.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def update_topic(
        self,
        topic: Topic,
        timeout: Optional[float] = None,
    ) -> Topic:
        """Update a topic.
        
        Args:
            topic: The updated topic.
            timeout: Optional timeout in seconds.
            
        Returns:
            The updated topic.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_topic(
        self,
        topic_path: Union[TopicPath, str],
        timeout: Optional[float] = None,
    ) -> None:
        """Delete a topic.
        
        Args:
            topic_path: Path or name of the topic to delete.
            timeout: Optional timeout in seconds.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def create_subscription(
        self,
        subscription: Subscription,
        timeout: Optional[float] = None,
    ) -> Subscription:
        """Create a subscription.
        
        Args:
            subscription: The subscription to create.
            timeout: Optional timeout in seconds.
            
        Returns:
            The created subscription.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def get_subscription(
        self,
        subscription_path: Union[SubscriptionPath, str],
        timeout: Optional[float] = None,
    ) -> Subscription:
        """Get a subscription.
        
        Args:
            subscription_path: Path or name of the subscription.
            timeout: Optional timeout in seconds.
            
        Returns:
            The subscription.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def list_subscriptions(
        self,
        location_path: LocationPath,
        timeout: Optional[float] = None,
    ) -> List[Subscription]:
        """List subscriptions in a location.
        
        Args:
            location_path: The location to list subscriptions in.
            timeout: Optional timeout in seconds.
            
        Returns:
            List of subscriptions.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def update_subscription(
        self,
        subscription: Subscription,
        timeout: Optional[float] = None,
    ) -> Subscription:
        """Update a subscription.
        
        Args:
            subscription: The updated subscription.
            timeout: Optional timeout in seconds.
            
        Returns:
            The updated subscription.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_subscription(
        self,
        subscription_path: Union[SubscriptionPath, str],
        timeout: Optional[float] = None,
    ) -> None:
        """Delete a subscription.
        
        Args:
            subscription_path: Path or name of the subscription to delete.
            timeout: Optional timeout in seconds.
        """
        raise NotImplementedError