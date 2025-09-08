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

from typing import Optional, List, Union

from google.api_core.client_options import ClientOptions
from google.auth.credentials import Credentials
from overrides import overrides

from google.cloud.pubsublite.transport.transport_interface import AdminTransport
from google.cloud.pubsublite.internal.wire.admin_client_impl import AdminClientImpl
from google.cloud.pubsublite.types import (
    TopicPath,
    SubscriptionPath,
    LocationPath,
    CloudRegion,
)
from google.cloud.pubsublite_v1.types.common import Topic, Subscription


class PubSubLiteAdminTransport(AdminTransport):
    """Pub/Sub Lite implementation of AdminTransport."""
    
    def __init__(
        self,
        region: str,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
        **kwargs
    ):
        """Initialize Pub/Sub Lite admin transport.
        
        Args:
            region: Cloud region for the admin client.
            credentials: Google Cloud credentials.
            client_options: Client options.
            **kwargs: Additional arguments.
        """
        self._region = CloudRegion(region)
        self._credentials = credentials
        self._client_options = client_options
        self._kwargs = kwargs
        self._admin_client = None
    
    def _get_admin_client(self) -> AdminClientImpl:
        """Get or create the underlying Pub/Sub Lite admin client."""
        if self._admin_client is None:
            self._admin_client = AdminClientImpl(
                self._region,
                credentials=self._credentials,
                client_options=self._client_options,
                **self._kwargs
            )
        return self._admin_client
    
    @overrides
    def create_topic(
        self,
        topic: Topic,
        timeout: Optional[float] = None,
    ) -> Topic:
        """Create a topic."""
        admin_client = self._get_admin_client()
        return admin_client.create_topic(topic, timeout=timeout)
    
    @overrides
    def get_topic(
        self,
        topic_path: Union[TopicPath, str],
        timeout: Optional[float] = None,
    ) -> Topic:
        """Get a topic."""
        admin_client = self._get_admin_client()
        if isinstance(topic_path, str):
            topic_path = TopicPath.parse(topic_path)
        return admin_client.get_topic(topic_path, timeout=timeout)
    
    @overrides
    def list_topics(
        self,
        location_path: LocationPath,
        timeout: Optional[float] = None,
    ) -> List[Topic]:
        """List topics in a location."""
        admin_client = self._get_admin_client()
        response = admin_client.list_topics(location_path, timeout=timeout)
        return list(response)
    
    @overrides
    def update_topic(
        self,
        topic: Topic,
        timeout: Optional[float] = None,
    ) -> Topic:
        """Update a topic."""
        admin_client = self._get_admin_client()
        return admin_client.update_topic(topic, timeout=timeout)
    
    @overrides
    def delete_topic(
        self,
        topic_path: Union[TopicPath, str],
        timeout: Optional[float] = None,
    ) -> None:
        """Delete a topic."""
        admin_client = self._get_admin_client()
        if isinstance(topic_path, str):
            topic_path = TopicPath.parse(topic_path)
        admin_client.delete_topic(topic_path, timeout=timeout)
    
    @overrides
    def create_subscription(
        self,
        subscription: Subscription,
        timeout: Optional[float] = None,
    ) -> Subscription:
        """Create a subscription."""
        admin_client = self._get_admin_client()
        return admin_client.create_subscription(subscription, timeout=timeout)
    
    @overrides
    def get_subscription(
        self,
        subscription_path: Union[SubscriptionPath, str],
        timeout: Optional[float] = None,
    ) -> Subscription:
        """Get a subscription."""
        admin_client = self._get_admin_client()
        if isinstance(subscription_path, str):
            subscription_path = SubscriptionPath.parse(subscription_path)
        return admin_client.get_subscription(subscription_path, timeout=timeout)
    
    @overrides
    def list_subscriptions(
        self,
        location_path: LocationPath,
        timeout: Optional[float] = None,
    ) -> List[Subscription]:
        """List subscriptions in a location."""
        admin_client = self._get_admin_client()
        response = admin_client.list_subscriptions(location_path, timeout=timeout)
        return list(response)
    
    @overrides
    def update_subscription(
        self,
        subscription: Subscription,
        timeout: Optional[float] = None,
    ) -> Subscription:
        """Update a subscription."""
        admin_client = self._get_admin_client()
        return admin_client.update_subscription(subscription, timeout=timeout)
    
    @overrides
    def delete_subscription(
        self,
        subscription_path: Union[SubscriptionPath, str],
        timeout: Optional[float] = None,
    ) -> None:
        """Delete a subscription."""
        admin_client = self._get_admin_client()
        if isinstance(subscription_path, str):
            subscription_path = SubscriptionPath.parse(subscription_path)
        admin_client.delete_subscription(subscription_path, timeout=timeout)