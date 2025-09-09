# Copyright 2024 Google LLC
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

"""Integration tests for PublisherClient with Kafka backend."""

from unittest.mock import patch, MagicMock, ANY
import pytest

# Skip all tests if Kafka is not available
pytest.importorskip("confluent_kafka")

from google.cloud.pubsublite.cloudpubsub.publisher_client import PublisherClient
from google.cloud.pubsublite.cloudpubsub.internal.make_publisher import make_publisher


@patch('google.cloud.pubsublite.cloudpubsub.internal.make_publisher.make_publisher')
def test_kafka_client_creation(mock_make_publisher):
    """Test PublisherClient creates Kafka backend correctly."""
    mock_make_publisher.return_value = MagicMock()
    
    client = PublisherClient(
        use_kafka=True,
        kafka_producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    # Verify make_publisher was called with correct parameters
    mock_make_publisher.assert_called_once()
    call_args = mock_make_publisher.call_args
    
    # Should use Kafka backend
    assert call_args[1]['use_kafka'] is True
    
    # Should pass kafka_config
    kafka_config = call_args[1]['kafka_config']
    assert kafka_config is not None
    assert kafka_config.producer_config['bootstrap.servers'] == 'localhost:9092'


def test_kafka_requires_bootstrap_servers():
    """Test that Kafka backend requires bootstrap.servers."""
    with pytest.raises(ValueError, match="bootstrap.servers"):
        PublisherClient(use_kafka=True, kafka_producer_config={})
    
    with pytest.raises(ValueError, match="bootstrap.servers"):
        PublisherClient(use_kafka=True, kafka_producer_config={'client.id': 'test'})
    
    with pytest.raises(ValueError, match="bootstrap.servers"):
        PublisherClient(use_kafka=True, kafka_producer_config=None)


def test_pubsublite_client_creation():
    """Test PublisherClient creates Pub/Sub Lite backend correctly."""
    with patch('google.cloud.pubsublite.cloudpubsub.internal.make_publisher.make_publisher') as mock_make_publisher:
        mock_make_publisher.return_value = MagicMock()
        
        # Default should be Pub/Sub Lite
        client = PublisherClient()
        
        mock_make_publisher.assert_called_once()
        call_args = mock_make_publisher.call_args
        
        # Should use Pub/Sub Lite backend
        assert call_args[1]['use_kafka'] is False
        assert call_args[1]['kafka_config'] is None


def test_environment_variable_backend_selection():
    """Test that PUBSUBLITE_USE_KAFKA environment variable works."""
    import os
    
    with patch.dict(os.environ, {'PUBSUBLITE_USE_KAFKA': 'true'}):
        with patch('google.cloud.pubsublite.cloudpubsub.internal.make_publisher.make_publisher') as mock_make_publisher:
            mock_make_publisher.return_value = MagicMock()
            
            # Should detect Kafka from environment
            with pytest.raises(ValueError, match="bootstrap.servers"):
                # Will fail because we need kafka_producer_config
                PublisherClient()


def test_explicit_false_overrides_environment():
    """Test that explicit use_kafka=False overrides environment."""
    import os
    
    with patch.dict(os.environ, {'PUBSUBLITE_USE_KAFKA': 'true'}):
        with patch('google.cloud.pubsublite.cloudpubsub.internal.make_publisher.make_publisher') as mock_make_publisher:
            mock_make_publisher.return_value = MagicMock()
            
            # Explicit False should override environment
            client = PublisherClient(use_kafka=False)
            
            call_args = mock_make_publisher.call_args
            assert call_args[1]['use_kafka'] is False


@patch('google.cloud.pubsublite.cloudpubsub.internal.make_publisher.make_publisher')
def test_kafka_config_parameters_passed_through(mock_make_publisher):
    """Test that all parameters are passed through to make_publisher."""
    mock_make_publisher.return_value = MagicMock()
    
    from google.auth.credentials import Credentials
    mock_credentials = MagicMock(spec=Credentials)
    
    client = PublisherClient(
        use_kafka=True,
        kafka_producer_config={'bootstrap.servers': 'server1:9092,server2:9092'},
        credentials=mock_credentials,
        enable_idempotence=True
    )
    
    call_args = mock_make_publisher.call_args
    
    # Check that credentials were passed
    assert call_args[1]['credentials'] is mock_credentials
    
    # Check that enable_idempotence created a client_id
    assert call_args[1]['client_id'] is not None
    
    # Check kafka_config has correct settings
    kafka_config = call_args[1]['kafka_config']
    assert kafka_config.credentials is mock_credentials
    assert 'server1:9092,server2:9092' in kafka_config.producer_config['bootstrap.servers']


def test_async_publisher_client_kafka_creation():
    """Test AsyncPublisherClient with Kafka backend."""
    from google.cloud.pubsublite.cloudpubsub.publisher_client import AsyncPublisherClient
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.make_publisher.make_async_publisher') as mock_make_async:
        mock_make_async.return_value = MagicMock()
        
        client = AsyncPublisherClient(
            use_kafka=True,
            kafka_producer_config={'bootstrap.servers': 'localhost:9092'}
        )
        
        # Verify make_async_publisher was called with Kafka backend
        mock_make_async.assert_called_once()
        call_args = mock_make_async.call_args
        assert call_args[1]['use_kafka'] is True
        assert call_args[1]['kafka_config'] is not None