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
from unittest.mock import Mock, patch
from concurrent.futures import Future

from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
from google.cloud.pubsublite.transport import TransportType, MSAKConfig
from google.cloud.pubsublite.types import TopicPath


class TestEnhancedPublisherClient:
    """Tests for EnhancedPublisherClient."""
    
    def test_create_for_pubsub_lite(self):
        """Test creating client for Pub/Sub Lite."""
        topic_path = TopicPath.parse("projects/test/locations/us-central1/topics/test-topic")
        
        client = EnhancedPublisherClient.create_for_pubsub_lite(topic_path)
        
        assert client.transport_type == TransportType.PUBSUB_LITE
        assert client._topic == topic_path
    
    def test_create_for_msak(self):
        """Test creating client for MSAK (will fail without library)."""
        msak_config = MSAKConfig(
            project_id="test-project",
            location="us-central1",
            cluster_id="test-cluster",
        )
        
        with pytest.raises(ImportError, match="google-cloud-managed-kafka is required"):
            EnhancedPublisherClient.create_for_msak(
                topic="test-topic",
                msak_config=msak_config,
            )
    
    @patch('google.cloud.pubsublite.transport.TransportFactory.create_publisher_transport')
    def test_publish_delegates_to_transport(self, mock_factory):
        """Test that publish delegates to the underlying transport."""
        # Setup mock transport
        mock_transport = Mock()
        mock_future = Future()
        mock_future.set_result("message_id_123")
        mock_transport.publish.return_value = mock_future
        mock_factory.return_value = mock_transport
        
        topic_path = TopicPath.parse("projects/test/locations/us-central1/topics/test-topic")
        
        with EnhancedPublisherClient(topic_path) as client:
            future = client.publish(topic_path, b"test message", ordering_key="key1", attr1="value1")
            
            result = future.result()
            assert result == "message_id_123"
            
            # Verify transport was called correctly
            mock_transport.publish.assert_called_once()
            call_args = mock_transport.publish.call_args
            message, ordering_key = call_args[0]
            
            assert message.data == b"test message"
            assert message.ordering_key == "key1"
            assert message.attributes["attr1"] == "value1"
            assert ordering_key == "key1"
    
    @patch('google.cloud.pubsublite.transport.TransportFactory.create_publisher_transport')
    def test_publish_validates_topic_match(self, mock_factory):
        """Test that publish validates topic matches client configuration."""
        mock_transport = Mock()
        mock_factory.return_value = mock_transport
        
        topic_path = TopicPath.parse("projects/test/locations/us-central1/topics/test-topic")
        wrong_topic = TopicPath.parse("projects/test/locations/us-central1/topics/wrong-topic")
        
        with EnhancedPublisherClient(topic_path) as client:
            with pytest.raises(ValueError, match="Topic .* doesn't match client topic"):
                client.publish(wrong_topic, b"test message")
    
    @patch('google.cloud.pubsublite.transport.TransportFactory.create_publisher_transport')
    def test_context_manager_lifecycle(self, mock_factory):
        """Test that context manager properly starts/stops the transport."""
        mock_transport = Mock()
        mock_factory.return_value = mock_transport
        
        topic_path = TopicPath.parse("projects/test/locations/us-central1/topics/test-topic")
        
        with EnhancedPublisherClient(topic_path) as client:
            # Should call transport.__enter__
            mock_transport.__enter__.assert_called_once()
            
            # Client should be started
            assert client._require_started._started
        
        # Should call transport.__exit__
        mock_transport.__exit__.assert_called_once()
    
    @patch('google.cloud.pubsublite.transport.TransportFactory.create_publisher_transport')
    def test_flush_delegates_to_transport(self, mock_factory):
        """Test that flush delegates to transport if supported."""
        mock_transport = Mock()
        mock_transport.flush = Mock()
        mock_factory.return_value = mock_transport
        
        topic_path = TopicPath.parse("projects/test/locations/us-central1/topics/test-topic")
        
        with EnhancedPublisherClient(topic_path) as client:
            client.flush(timeout=30.0)
            
            mock_transport.flush.assert_called_once_with(30.0)
    
    @patch('google.cloud.pubsublite.transport.TransportFactory.create_publisher_transport') 
    def test_flush_handles_transport_without_flush(self, mock_factory):
        """Test that flush gracefully handles transport without flush method."""
        mock_transport = Mock()
        # Don't add flush method to mock
        delattr(type(mock_transport), 'flush')
        mock_factory.return_value = mock_transport
        
        topic_path = TopicPath.parse("projects/test/locations/us-central1/topics/test-topic")
        
        with EnhancedPublisherClient(topic_path) as client:
            # Should not raise an error
            client.flush(timeout=30.0)
    
    def test_transport_type_property(self):
        """Test that transport_type property returns correct type."""
        topic_path = TopicPath.parse("projects/test/locations/us-central1/topics/test-topic")
        
        client = EnhancedPublisherClient(topic_path, transport_type=TransportType.PUBSUB_LITE)
        assert client.transport_type == TransportType.PUBSUB_LITE
        
        # Test with MSAK (will fail due to missing dependency, but type should be set)
        try:
            msak_config = MSAKConfig(
                project_id="test-project",
                location="us-central1", 
                cluster_id="test-cluster",
            )
            client = EnhancedPublisherClient(
                "test-topic",
                transport_type=TransportType.MANAGED_KAFKA,
                msak_config=msak_config,
            )
        except ImportError:
            # Expected - we don't have the MSAK library
            pass