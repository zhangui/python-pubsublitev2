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

"""Tests for Kafka configuration validation and builders."""

import pytest
from datetime import timedelta
from unittest.mock import MagicMock, patch

from google.cloud.pubsublite.cloudpubsub.publisher_client import _create_kafka_config
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import (
    KafkaConfig,
    KafkaConfigBuilder,
    create_oauth_token_callback,
)
from google.cloud.pubsub_v1.types import BatchSettings


def test_kafka_config_creation():
    """Test basic KafkaConfig creation."""
    config = KafkaConfig(
        credentials=None,
        producer_config={'bootstrap.servers': 'localhost:9092', 'client.id': 'test'}
    )
    
    assert config.credentials is None
    assert config.producer_config['bootstrap.servers'] == 'localhost:9092'
    assert config.producer_config['client.id'] == 'test'


def test_create_kafka_config_requires_complete_config():
    """Test that _create_kafka_config requires complete configuration."""
    # Should raise error when producer_config is missing
    with pytest.raises(ValueError, match="kafka_producer_config is required"):
        _create_kafka_config(producer_config=None)
    
    # Should raise error when bootstrap.servers is missing
    with pytest.raises(ValueError, match="bootstrap.servers"):
        _create_kafka_config(producer_config={})
    
    # Should raise error when security.protocol is missing
    with pytest.raises(ValueError, match="security.protocol"):
        _create_kafka_config(producer_config={'bootstrap.servers': 'localhost:9092'})
    
    # Should raise error when SASL config is incomplete
    with pytest.raises(ValueError, match="sasl.mechanisms"):
        _create_kafka_config(producer_config={
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'SASL_SSL'
        })
    
    # Should raise error when OAuth callback is missing
    with pytest.raises(ValueError, match="oauth_cb"):
        _create_kafka_config(producer_config={
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER'
        })


def test_create_kafka_config_mtls_validation():
    """Test mTLS configuration validation."""
    # Should raise error when SSL config is incomplete - missing ssl.key.location
    with pytest.raises(ValueError, match="mTLS"):
        _create_kafka_config(producer_config={
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'SSL',
            'ssl.certificate.location': '/path/to/cert.pem',
            'ssl.ca.location': '/path/to/ca.pem'
            # Missing ssl.key.location
        })
    
    # Should raise error when SSL config is incomplete - missing ssl.ca.location
    with pytest.raises(ValueError, match="mTLS"):
        _create_kafka_config(producer_config={
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'SSL',
            'ssl.certificate.location': '/path/to/cert.pem',
            'ssl.key.location': '/path/to/key.pem'
            # Missing ssl.ca.location
        })


def test_create_kafka_config_with_valid_oauth():
    """Test _create_kafka_config with valid OAuth configuration."""
    mock_credentials = MagicMock()
    
    def mock_oauth_cb(config):
        return ("token", 1234567890)
    
    kafka_config = _create_kafka_config(
        credentials=mock_credentials,
        producer_config={
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': mock_oauth_cb
        }
    )
    
    assert isinstance(kafka_config, KafkaConfig)
    assert kafka_config.producer_config['bootstrap.servers'] == 'localhost:9092'
    assert kafka_config.producer_config['security.protocol'] == 'SASL_SSL'
    assert callable(kafka_config.producer_config['oauth_cb'])


def test_create_kafka_config_with_valid_mtls():
    """Test _create_kafka_config with valid mTLS configuration."""
    kafka_config = _create_kafka_config(
        producer_config={
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'SSL',
            'ssl.certificate.location': '/path/to/cert.pem',
            'ssl.key.location': '/path/to/key.pem',
            'ssl.ca.location': '/path/to/ca.pem'
        }
    )
    
    assert isinstance(kafka_config, KafkaConfig)
    assert kafka_config.producer_config['security.protocol'] == 'SSL'
    assert kafka_config.producer_config['ssl.certificate.location'] == '/path/to/cert.pem'


def test_batch_settings_conversion():
    """Test BatchSettings to Kafka config conversion."""
    batch_settings = BatchSettings(
        max_messages=100,
        max_bytes=1024 * 1024,  # 1MB
        max_latency=timedelta(milliseconds=10)
    )
    
    config = KafkaConfigBuilder.from_batch_settings(batch_settings)
    
    assert config['batch.num.messages'] == 100
    assert config['batch.size'] == 1024 * 1024
    assert config['linger.ms'] == 10
    assert config['queue.buffering.max.messages'] == 1000  # 10x batch size


def test_build_oauth_config():
    """Test OAuth configuration builder."""
    mock_credentials = MagicMock()
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.kafka_config.create_oauth_token_callback') as mock_create:
        mock_create.return_value = lambda x: ("token", 123456)
        
        config = KafkaConfigBuilder.build_oauth_config(
            bootstrap_servers='server1:9092,server2:9092',
            credentials=mock_credentials
        )
        
        assert config['bootstrap.servers'] == 'server1:9092,server2:9092'
        assert config['security.protocol'] == 'SASL_SSL'
        assert config['sasl.mechanisms'] == 'OAUTHBEARER'
        assert callable(config['oauth_cb'])
        mock_create.assert_called_once_with(mock_credentials)


def test_build_mtls_config():
    """Test mTLS configuration builder."""
    config = KafkaConfigBuilder.build_mtls_config(
        bootstrap_servers='server1:9092',
        cert_location='/path/to/cert.pem',
        key_location='/path/to/key.pem',
        ca_location='/path/to/ca.pem',
        key_password='secret'
    )
    
    assert config['bootstrap.servers'] == 'server1:9092'
    assert config['security.protocol'] == 'SSL'
    assert config['ssl.certificate.location'] == '/path/to/cert.pem'
    assert config['ssl.key.location'] == '/path/to/key.pem'
    assert config['ssl.ca.location'] == '/path/to/ca.pem'
    assert config['ssl.key.password'] == 'secret'
    assert config['ssl.endpoint.identification.algorithm'] == 'https'


def test_add_reliability_settings():
    """Test reliability settings builder."""
    base_config = {'bootstrap.servers': 'localhost:9092'}
    
    # With idempotence
    config = KafkaConfigBuilder.add_reliability_settings(base_config, enable_idempotence=True)
    assert config['enable.idempotence'] is True
    assert config['acks'] == 'all'
    assert config['retries'] == 2147483647
    assert config['max.in.flight.requests.per.connection'] == 5
    assert config['delivery.timeout.ms'] == 120000
    
    # Without idempotence
    config = KafkaConfigBuilder.add_reliability_settings(base_config, enable_idempotence=False)
    assert 'enable.idempotence' not in config or config['enable.idempotence'] is False
    assert config['acks'] == 'all'
    assert config['retries'] == 3


def test_add_performance_settings():
    """Test performance settings builder."""
    base_config = {'bootstrap.servers': 'localhost:9092'}
    
    config = KafkaConfigBuilder.add_performance_settings(base_config, compression='lz4')
    
    assert config['compression.type'] == 'lz4'
    assert config['batch.size'] == 1000000
    assert config['linger.ms'] == 5
    assert config['buffer.memory'] == 33554432
    assert config['queue.buffering.max.messages'] == 100000
    assert config['queue.buffering.max.kbytes'] == 1048576


def test_oauth_token_callback():
    """Test OAuth token callback function."""
    mock_credentials = MagicMock()
    mock_credentials.token = 'test-token'
    mock_credentials.expired = False
    
    # Mock expiry timestamp
    import time
    mock_credentials.expiry = MagicMock()
    mock_credentials.expiry.timestamp.return_value = time.time() + 3600
    
    oauth_cb = create_oauth_token_callback(mock_credentials)
    
    # Call the callback
    token, expiry_ms = oauth_cb("config-string")
    
    assert token == 'test-token'
    assert expiry_ms > int(time.time() * 1000)  # Should be in future


def test_oauth_token_callback_refresh():
    """Test OAuth token callback refreshes expired token."""
    mock_credentials = MagicMock()
    mock_credentials.token = None
    mock_credentials.expired = True
    
    oauth_cb = create_oauth_token_callback(mock_credentials)
    
    # Set up refreshed token
    mock_credentials.token = 'refreshed-token'
    
    with patch('google.cloud.pubsublite.cloudpubsub.internal.kafka_config.Request'):
        token, expiry_ms = oauth_cb("config")
        
        # Should have called refresh
        mock_credentials.refresh.assert_called_once()
        assert token == 'refreshed-token'


def test_create_kafka_config_with_batch_settings():
    """Test that batch settings are applied to Kafka config."""
    batch_settings = BatchSettings(
        max_messages=50,
        max_bytes=512 * 1024,
        max_latency=timedelta(milliseconds=5)
    )
    
    kafka_config = _create_kafka_config(
        producer_config={
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': lambda x: ("token", 123456)
        },
        batch_settings=batch_settings
    )
    
    # Batch settings should be applied
    assert kafka_config.producer_config['batch.num.messages'] == 50
    assert kafka_config.producer_config['batch.size'] == 512 * 1024
    assert kafka_config.producer_config['linger.ms'] == 5