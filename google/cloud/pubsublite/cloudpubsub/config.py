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

"""
Configuration utilities for Universal Publisher Client.

This module provides utilities to configure the UniversalPublisherClient
for use with either Pub/Sub Lite or Google Managed Service for Apache Kafka.
"""

import os
from typing import List, Dict, Any, Optional
from google.auth.credentials import Credentials
from google.auth import default


def get_kafka_bootstrap_servers_from_env() -> List[str]:
    """
    Get Kafka bootstrap servers from environment variable.
    
    Returns:
        List of bootstrap server addresses from KAFKA_BOOTSTRAP_SERVERS
        environment variable (comma-separated).
        
    Raises:
        ValueError: If KAFKA_BOOTSTRAP_SERVERS is not set or empty.
    """
    servers_str = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    if not servers_str:
        raise ValueError(
            "KAFKA_BOOTSTRAP_SERVERS environment variable must be set when using Kafka backend"
        )
    
    return [server.strip() for server in servers_str.split(',')]


def get_kafka_auth_endpoint_from_env() -> str:
    """
    Get Kafka OAuth authentication endpoint from environment variable.
    
    Returns:
        OAuth endpoint URL from KAFKA_AUTH_ENDPOINT environment variable,
        or default localhost:14293 if not set.
    """
    return os.getenv('KAFKA_AUTH_ENDPOINT', 'localhost:14293')


def create_kafka_producer_config() -> Dict[str, Any]:
    """
    Create default Kafka producer configuration optimized for Google Cloud.
    
    Returns:
        Dictionary with recommended Kafka producer settings.
    """
    return {
        # Performance settings
        'batch.size': 16384,
        'linger.ms': 5,
        'compression.type': 'snappy',
        
        # Reliability settings
        'acks': 'all',
        'retries': 3,
        'retry.backoff.ms': 100,
        'max.in.flight.requests.per.connection': 5,
        'enable.idempotence': True,
        
        # Timeout settings
        'request.timeout.ms': 30000,
        'delivery.timeout.ms': 120000,
        
        # Buffer settings
        'buffer.memory': 33554432,  # 32MB
        'send.buffer.bytes': 131072,  # 128KB
        'receive.buffer.bytes': 65536,  # 64KB
    }


def is_kafka_backend_requested() -> bool:
    """
    Check if Kafka backend is requested via environment variables.
    
    Returns:
        True if PUBSUBLITE_USE_KAFKA environment variable is set to a truthy value.
    """
    env_var = os.getenv('PUBSUBLITE_USE_KAFKA', '').lower()
    return env_var in ('true', '1', 'yes')


def get_google_cloud_credentials() -> Credentials:
    """
    Get Google Cloud credentials using Application Default Credentials (ADC).
    
    Returns:
        Google Cloud credentials instance.
        
    Raises:
        google.auth.exceptions.DefaultCredentialsError: If credentials cannot be found.
    """
    credentials, _ = default()
    return credentials


# Configuration templates for common scenarios

def get_local_kafka_config():
    """
    Get configuration for local Kafka development setup.
    
    Returns:
        Dict with configuration suitable for local Kafka development.
    """
    try:
        from google.cloud.pubsublite.cloudpubsub.msak_client import KafkaConfig
    except ImportError:
        raise ImportError("Kafka functionality not available. Install with: pip install google-cloud-pubsublite[kafka]")
    
    return KafkaConfig(
        bootstrap_servers=['localhost:9092'],
        auth_endpoint='localhost:14293',
        credentials=get_google_cloud_credentials(),
        producer_config=create_kafka_producer_config(),
    )


def get_gcp_kafka_config(
    cluster_bootstrap_servers: List[str],
    auth_endpoint: str = "localhost:14293",
    credentials: Optional[Credentials] = None,
) -> 'KafkaConfig':
    """
    Get configuration for Google Cloud Managed Service for Apache Kafka.
    
    Args:
        cluster_bootstrap_servers: List of bootstrap server addresses for your Kafka cluster.
        auth_endpoint: OAuth authentication endpoint (default: localhost:14293).
        credentials: Google Cloud credentials. Uses ADC if not provided.
        
    Returns:
        KafkaConfig instance configured for Google Cloud.
    """
    try:
        from google.cloud.pubsublite.cloudpubsub.msak_client import KafkaConfig
    except ImportError:
        raise ImportError("Kafka functionality not available. Install with: pip install google-cloud-pubsublite[kafka]")
    
    return KafkaConfig(
        bootstrap_servers=cluster_bootstrap_servers,
        auth_endpoint=auth_endpoint,
        credentials=credentials or get_google_cloud_credentials(),
        producer_config=create_kafka_producer_config(),
    )