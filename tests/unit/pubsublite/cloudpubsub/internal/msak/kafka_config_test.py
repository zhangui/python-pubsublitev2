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

"""Tests for Kafka configuration validation."""

import pytest

from google.cloud.pubsublite.cloudpubsub.publisher_client import _create_kafka_config
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import (
    KafkaConfig,
    create_default_kafka_producer_config,
)


def test_kafka_config_creation():
    """Test basic KafkaConfig creation."""
    config = KafkaConfig(
        credentials=None,
        producer_config={'bootstrap.servers': 'localhost:9092', 'client.id': 'test'}
    )
    
    assert config.credentials is None
    assert config.producer_config['bootstrap.servers'] == 'localhost:9092'
    assert config.producer_config['client.id'] == 'test'


def test_kafka_config_empty_producer_config():
    """Test KafkaConfig with empty producer_config."""
    config = KafkaConfig(producer_config={})
    assert config.producer_config == {}


def test_kafka_config_none_producer_config():
    """Test KafkaConfig with None producer_config."""
    config = KafkaConfig(producer_config=None)
    assert config.producer_config == {}


def test_create_kafka_config_requires_bootstrap_servers():
    """Test that _create_kafka_config requires bootstrap.servers."""
    # Should raise error when bootstrap.servers is missing
    with pytest.raises(ValueError, match="bootstrap.servers"):
        _create_kafka_config(producer_config={})
    
    with pytest.raises(ValueError, match="bootstrap.servers"):
        _create_kafka_config(producer_config={'client.id': 'test'})
    
    with pytest.raises(ValueError, match="bootstrap.servers"):
        _create_kafka_config(producer_config=None)


def test_create_kafka_config_with_bootstrap_servers():
    """Test that _create_kafka_config works with bootstrap.servers."""
    kafka_config = _create_kafka_config(
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    assert isinstance(kafka_config, KafkaConfig)
    assert kafka_config.producer_config['bootstrap.servers'] == 'localhost:9092'
    # Should merge with defaults
    assert 'acks' in kafka_config.producer_config
    assert 'retries' in kafka_config.producer_config


def test_create_kafka_config_merges_defaults():
    """Test that user config is merged with defaults."""
    user_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'custom-client',
        'acks': 1,  # Override default
    }
    
    kafka_config = _create_kafka_config(producer_config=user_config)
    
    # User settings should be preserved
    assert kafka_config.producer_config['bootstrap.servers'] == 'localhost:9092'
    assert kafka_config.producer_config['client.id'] == 'custom-client'
    assert kafka_config.producer_config['acks'] == 1  # User override
    
    # Default settings should be included
    assert 'retries' in kafka_config.producer_config
    assert 'compression.type' in kafka_config.producer_config


def test_default_kafka_producer_config():
    """Test that default producer config has expected settings."""
    defaults = create_default_kafka_producer_config()
    
    # Performance settings
    assert defaults['batch.size'] == 16384
    assert defaults['linger.ms'] == 5
    assert defaults['compression.type'] == 'snappy'
    
    # Reliability settings
    assert defaults['acks'] == 'all'
    assert defaults['retries'] == 3
    assert defaults['enable.idempotence'] is True
    
    # Timeout settings
    assert defaults['request.timeout.ms'] == 30000
    assert defaults['delivery.timeout.ms'] == 120000


def test_user_config_overrides_defaults():
    """Test that user configuration overrides defaults."""
    user_config = {
        'bootstrap.servers': 'server1:9092,server2:9092',
        'acks': 1,  # Override default 'all'
        'retries': 10,  # Override default 3
        'custom.setting': 'custom-value',
    }
    
    kafka_config = _create_kafka_config(producer_config=user_config)
    config = kafka_config.producer_config
    
    # User overrides should take precedence
    assert config['acks'] == 1
    assert config['retries'] == 10
    assert config['custom.setting'] == 'custom-value'
    
    # Other defaults should remain
    assert config['enable.idempotence'] is True
    assert config['compression.type'] == 'snappy'


def test_credentials_passed_through():
    """Test that credentials are passed through to KafkaConfig."""
    from unittest.mock import MagicMock
    
    mock_credentials = MagicMock()
    kafka_config = _create_kafka_config(
        credentials=mock_credentials,
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )
    
    assert kafka_config.credentials is mock_credentials