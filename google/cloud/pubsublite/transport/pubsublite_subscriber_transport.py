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

from typing import Optional, Callable

from google.api_core.client_options import ClientOptions
from google.auth.credentials import Credentials
from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from overrides import overrides

from google.cloud.pubsublite.transport.transport_interface import SubscriberTransport
from google.cloud.pubsublite.cloudpubsub.internal.make_subscriber import make_async_subscriber
from google.cloud.pubsublite.types import SubscriptionPath, FlowControlSettings


class PubSubLiteSubscriberTransport(SubscriberTransport):
    """Pub/Sub Lite implementation of SubscriberTransport."""
    
    def __init__(
        self,
        subscription: SubscriptionPath,
        flow_control_settings: Optional[FlowControlSettings] = None,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
        **kwargs
    ):
        """Initialize Pub/Sub Lite subscriber transport.
        
        Args:
            subscription: Pub/Sub Lite subscription path to subscribe to.
            flow_control_settings: Flow control configuration.
            credentials: Google Cloud credentials.
            client_options: Client options.
            **kwargs: Additional arguments passed to make_async_subscriber.
        """
        self._subscription = subscription
        self._flow_control_settings = flow_control_settings
        self._credentials = credentials
        self._client_options = client_options
        self._kwargs = kwargs
        self._subscriber = None
    
    def _get_subscriber(self):
        """Get or create the underlying Pub/Sub Lite subscriber."""
        if self._subscriber is None:
            self._subscriber = make_async_subscriber(
                subscription=self._subscription,
                per_partition_flow_control_settings=self._flow_control_settings,
                credentials=self._credentials,
                client_options=self._client_options,
                **self._kwargs
            )
        return self._subscriber
    
    @overrides
    def subscribe(
        self,
        callback: Callable[[Message], None],
        flow_control: Optional[FlowControlSettings] = None,
    ) -> StreamingPullFuture:
        """Subscribe to messages.
        
        Args:
            callback: Callback function to handle received messages.
            flow_control: Optional flow control settings (overrides instance settings).
            
        Returns:
            Future representing the streaming pull operation.
        """
        # Update flow control if provided
        if flow_control:
            self._flow_control_settings = flow_control
            # Force recreation of subscriber with new settings
            self._subscriber = None
        
        subscriber = self._get_subscriber()
        
        # Start async subscriber and return future
        # Note: This is a simplified adapter - real implementation would need
        # proper async-to-sync bridging for the Pub/Sub Lite async subscriber
        return subscriber.start(callback)