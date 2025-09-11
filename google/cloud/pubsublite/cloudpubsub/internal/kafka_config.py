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

import time
import logging
from typing import Dict, Any, Optional, Tuple, Callable
from datetime import timedelta

from google.auth.credentials import Credentials
from google.auth import default
from google.auth.transport.requests import Request

logger = logging.getLogger(__name__)


class KafkaConfig:
    """
    Configuration for Kafka client connection.
    
    Users must provide complete producer configuration including:
    - bootstrap.servers: Kafka broker addresses
    - Authentication settings (OAuth or mTLS)
    - Reliability settings (acks, idempotence, etc.)
    - Performance tuning (batch.size, linger.ms, etc.)
    """
    
    def __init__(
        self,
        credentials: Optional[Credentials] = None,
        producer_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize Kafka configuration.
        
        Args:
            credentials: Google Cloud credentials for OAuth authentication
            producer_config: Complete Kafka producer configuration dictionary
        """
        self.credentials = credentials
        self.producer_config = producer_config or {}


def create_oauth_token_callback(
    credentials: Optional[Credentials] = None
) -> Callable[[str], Tuple[str, int]]:
    """
    Create an OAuth token callback function for confluent-kafka.
    
    This function creates a callback that retrieves access tokens from
    Google Application Default Credentials (ADC) for use with SASL/OAUTHBEARER
    authentication in Kafka.
    
    Args:
        credentials: Optional Google Cloud credentials. If not provided,
                    will use Application Default Credentials.
    
    Returns:
        A callback function that returns (token, expiry_ms) tuple
    """
    # Use provided credentials or get default
    creds = credentials
    if creds is None:
        creds, _ = default()
    
    def oauth_cb(oauth_config: str) -> Tuple[str, int]:
        """
        OAuth token callback for confluent-kafka.
        
        Args:
            oauth_config: Configuration string from sasl.oauthbearer.config
        
        Returns:
            Tuple of (access_token, expiry_time_ms)
        """
        try:
            # Refresh token if needed
            if not creds.token or (hasattr(creds, 'expired') and creds.expired):
                request = Request()
                creds.refresh(request)
            
            # Get the access token
            access_token = creds.token
            
            # Calculate expiry time in milliseconds
            # If expiry is available, use it; otherwise default to 1 hour
            if hasattr(creds, 'expiry') and creds.expiry:
                expiry_ms = int(creds.expiry.timestamp() * 1000)
            else:
                # Default to 1 hour expiry
                expiry_ms = int((time.time() + 3600) * 1000)
            
            logger.debug("OAuth token retrieved successfully")
            return (access_token, expiry_ms)
            
        except Exception as e:
            logger.error(f"Failed to retrieve OAuth token: {e}")
            # Return None to signal error to confluent-kafka
            return None
    
    return oauth_cb


class KafkaConfigBuilder:
    """
    Helper class to build Kafka producer configurations.
    """
    
    @staticmethod
    def from_batch_settings(batch_settings) -> Dict[str, Any]:
        """
        Convert Pub/Sub Lite BatchSettings to confluent-kafka configuration.
        
        Args:
            batch_settings: Pub/Sub Lite BatchSettings object
            
        Returns:
            Dictionary of confluent-kafka producer settings
        """
        config = {}
        
        # Convert max_messages to batch.num.messages
        if hasattr(batch_settings, 'max_messages') and batch_settings.max_messages:
            config['batch.num.messages'] = batch_settings.max_messages
        
        # Convert max_bytes to batch.size
        if hasattr(batch_settings, 'max_bytes') and batch_settings.max_bytes:
            config['batch.size'] = batch_settings.max_bytes
        
        # Convert max_latency to linger.ms
        if hasattr(batch_settings, 'max_latency'):
            if isinstance(batch_settings.max_latency, timedelta):
                config['linger.ms'] = int(batch_settings.max_latency.total_seconds() * 1000)
            elif batch_settings.max_latency:
                # Assume it's already in seconds
                config['linger.ms'] = int(batch_settings.max_latency * 1000)
        
        # Set queue buffering based on batch settings
        if hasattr(batch_settings, 'max_messages') and batch_settings.max_messages:
            # Set queue size to 10x batch size for better throughput
            config['queue.buffering.max.messages'] = batch_settings.max_messages * 10
        
        return config
    
    @staticmethod
    def build_oauth_config(
        bootstrap_servers: str,
        credentials: Optional[Credentials] = None,
    ) -> Dict[str, Any]:
        """
        Build OAuth/SASL configuration for confluent-kafka.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            credentials: Optional Google Cloud credentials
            
        Returns:
            Complete OAuth configuration for confluent-kafka Producer
        """
        # Create OAuth callback
        oauth_cb = create_oauth_token_callback(credentials)
        
        return {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': oauth_cb,
        }
    
    @staticmethod
    def build_mtls_config(
        bootstrap_servers: str,
        cert_location: str,
        key_location: str,
        ca_location: str,
        key_password: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Build mTLS configuration for confluent-kafka.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            cert_location: Path to client certificate file
            key_location: Path to client key file
            ca_location: Path to CA certificate file
            key_password: Optional password for encrypted key
            
        Returns:
            Complete mTLS configuration for confluent-kafka Producer
        """
        config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SSL',
            'ssl.certificate.location': cert_location,
            'ssl.key.location': key_location,
            'ssl.ca.location': ca_location,
            'ssl.endpoint.identification.algorithm': 'https',  # Enable hostname verification
        }
        
        if key_password:
            config['ssl.key.password'] = key_password
        
        return config
    
    @staticmethod
    def add_reliability_settings(
        config: Dict[str, Any],
        enable_idempotence: bool = True
    ) -> Dict[str, Any]:
        """
        Add reliability and delivery guarantee settings.
        
        Args:
            config: Existing configuration dictionary
            enable_idempotence: Whether to enable idempotent producer
            
        Returns:
            Updated configuration with reliability settings
        """
        reliability_config = config.copy()
        
        if enable_idempotence:
            reliability_config.update({
                'enable.idempotence': True,
                'acks': 'all',  # All in-sync replicas must acknowledge
                'retries': 2147483647,  # Max retries with idempotence
                'max.in.flight.requests.per.connection': 5,
            })
        else:
            reliability_config.update({
                'acks': 'all',
                'retries': 3,
            })
        
        # Delivery timeout settings
        reliability_config.update({
            'delivery.timeout.ms': 120000,  # 2 minutes total delivery timeout
            'request.timeout.ms': 30000,    # 30 seconds per request
        })
        
        return reliability_config
    
    @staticmethod
    def add_performance_settings(
        config: Dict[str, Any],
        compression: str = 'snappy'
    ) -> Dict[str, Any]:
        """
        Add performance tuning settings.
        
        Args:
            config: Existing configuration dictionary
            compression: Compression type ('none', 'gzip', 'snappy', 'lz4', 'zstd')
            
        Returns:
            Updated configuration with performance settings
        """
        perf_config = config.copy()
        
        perf_config.update({
            'compression.type': compression,
            'batch.size': 1000000,  # 1MB batch size
            'linger.ms': 5,  # 5ms linger time
            'buffer.memory': 33554432,  # 32MB buffer
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 1048576,  # 1GB
        })
        
        return perf_config