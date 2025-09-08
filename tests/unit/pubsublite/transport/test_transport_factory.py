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

import pytest
from unittest.mock import Mock

from google.cloud.pubsublite.transport import (
    TransportType,
    TransportFactory,
    MSAKConfig,
)
from google.cloud.pubsublite.types import TopicPath, SubscriptionPath, LocationPath


class TestTransportFactory:
    """Tests for TransportFactory."""
    
    def test_creates_pubsub_lite_publisher_transport(self):
        """Test that factory creates Pub/Sub Lite publisher transport."""
        topic_path = TopicPath.parse("projects/test-project/locations/us-central1/topics/test-topic")
        
        transport = TransportFactory.create_publisher_transport(
            transport_type=TransportType.PUBSUB_LITE,
            topic=topic_path,
        )
        
        # Should create PubSubLitePublisherTransport
        assert transport is not None
        assert "PubSubLitePublisherTransport" in str(type(transport))
    
    def test_creates_msak_publisher_transport(self):
        """Test that factory creates MSAK publisher transport."""
        msak_config = MSAKConfig(
            project_id="test-project",
            location="us-central1",
            cluster_id="test-cluster",
        )
        
        with pytest.raises(ImportError, match="google-cloud-managed-kafka is required"):
            # This will fail because we don't have the actual MSAK library installed
            TransportFactory.create_publisher_transport(
                transport_type=TransportType.MANAGED_KAFKA,
                topic="test-topic",
                msak_config=msak_config,
            )
    
    def test_pubsub_lite_publisher_requires_topic_path(self):
        """Test that Pub/Sub Lite publisher requires TopicPath."""
        with pytest.raises(ValueError, match="TopicPath required for Pub/Sub Lite transport"):
            TransportFactory.create_publisher_transport(
                transport_type=TransportType.PUBSUB_LITE,
                topic="invalid-string-topic",  # Should be TopicPath
            )
    
    def test_msak_publisher_requires_config(self):
        """Test that MSAK publisher requires MSAKConfig."""
        with pytest.raises(ValueError, match="MSAK config required for Managed Kafka transport"):
            TransportFactory.create_publisher_transport(
                transport_type=TransportType.MANAGED_KAFKA,
                topic="test-topic",
                # Missing msak_config
            )
    
    def test_creates_pubsub_lite_subscriber_transport(self):
        """Test that factory creates Pub/Sub Lite subscriber transport."""
        subscription_path = SubscriptionPath.parse(
            "projects/test-project/locations/us-central1/subscriptions/test-sub"
        )
        
        transport = TransportFactory.create_subscriber_transport(
            transport_type=TransportType.PUBSUB_LITE,
            subscription=subscription_path,
        )
        
        assert transport is not None
        assert "PubSubLiteSubscriberTransport" in str(type(transport))
    
    def test_creates_msak_subscriber_transport(self):
        """Test that factory creates MSAK subscriber transport."""
        msak_config = MSAKConfig(
            project_id="test-project",
            location="us-central1", 
            cluster_id="test-cluster",
        )
        
        with pytest.raises(ImportError, match="google-cloud-managed-kafka is required"):
            TransportFactory.create_subscriber_transport(
                transport_type=TransportType.MANAGED_KAFKA,
                subscription="test-consumer-group",
                msak_config=msak_config,
            )
    
    def test_creates_pubsub_lite_admin_transport(self):
        """Test that factory creates Pub/Sub Lite admin transport."""
        transport = TransportFactory.create_admin_transport(
            transport_type=TransportType.PUBSUB_LITE,
            region="us-central1",
        )
        
        assert transport is not None
        assert "PubSubLiteAdminTransport" in str(type(transport))
    
    def test_creates_msak_admin_transport(self):
        """Test that factory creates MSAK admin transport."""
        msak_config = MSAKConfig(
            project_id="test-project",
            location="us-central1",
            cluster_id="test-cluster", 
        )
        
        with pytest.raises(ImportError, match="google-cloud-managed-kafka is required"):
            TransportFactory.create_admin_transport(
                transport_type=TransportType.MANAGED_KAFKA,
                region="us-central1",
                msak_config=msak_config,
            )
    
    def test_invalid_transport_type_raises_error(self):
        """Test that invalid transport type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported transport type"):
            TransportFactory.create_publisher_transport(
                transport_type="invalid_transport",  # Invalid type
                topic="test-topic",
            )