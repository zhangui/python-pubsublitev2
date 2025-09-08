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
from typing import Optional

from google.api_core.client_options import ClientOptions
from google.auth.credentials import Credentials
from google.cloud.pubsub_v1.types import BatchSettings, PubsubMessage
from overrides import overrides

from google.cloud.pubsublite.transport.transport_interface import PublisherTransport
from google.cloud.pubsublite.cloudpubsub.internal.make_publisher import make_publisher
from google.cloud.pubsublite.types import TopicPath


class PubSubLitePublisherTransport(PublisherTransport):
    """Pub/Sub Lite implementation of PublisherTransport."""
    
    def __init__(
        self,
        topic: TopicPath,
        batch_settings: Optional[BatchSettings] = None,
        credentials: Optional[Credentials] = None,
        client_options: Optional[ClientOptions] = None,
        **kwargs
    ):
        """Initialize Pub/Sub Lite publisher transport.
        
        Args:
            topic: Pub/Sub Lite topic path to publish to.
            batch_settings: Batching configuration.
            credentials: Google Cloud credentials.
            client_options: Client options.
            **kwargs: Additional arguments passed to make_publisher.
        """
        self._topic = topic
        self._batch_settings = batch_settings
        self._credentials = credentials
        self._client_options = client_options
        self._kwargs = kwargs
        self._publisher = None
    
    def _get_publisher(self):
        """Get or create the underlying Pub/Sub Lite publisher."""
        if self._publisher is None:
            self._publisher = make_publisher(
                topic=self._topic,
                batch_settings=self._batch_settings,
                credentials=self._credentials,
                client_options=self._client_options,
                **self._kwargs
            )
        return self._publisher
    
    @overrides
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
        publisher = self._get_publisher()
        
        # Set ordering key if provided
        if ordering_key:
            message.ordering_key = ordering_key
        
        return publisher.publish(message)
    
    @overrides
    def __enter__(self):
        """Enter context manager."""
        publisher = self._get_publisher()
        publisher.__enter__()
        return self
    
    @overrides
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        if self._publisher:
            self._publisher.__exit__(exc_type, exc_val, exc_tb)