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

import logging
import asyncio
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Optional, Dict, Any, List
import time
import json

from google.cloud.pubsub_v1.types import BatchSettings, PubsubMessage
from google.auth.credentials import Credentials
from google.api_core.client_options import ClientOptions
from overrides import overrides

from google.cloud.pubsublite.transport.transport_interface import PublisherTransport
from google.cloud.pubsublite.transport.msak_config import MSAKConfig
from google.cloud.pubsublite.transport.message_adapters import MessageAdapter

try:
    from confluent_kafka import Producer, KafkaError, KafkaException
    from google.auth import default
    from google.auth.transport.requests import Request
    MSAK_AVAILABLE = True
except ImportError:
    MSAK_AVAILABLE = False
    Producer = None
    KafkaError = None

logger = logging.getLogger(__name__)


class MSAKPublisherTransport(PublisherTransport):
    """Google Managed Service for Apache Kafka implementation of PublisherTransport."""
    
    def __init__(
        self,
        topic: str,
        msak_config: MSAKConfig,
        batch_settings: Optional[BatchSettings] = None,
        client_options: Optional[ClientOptions] = None,
    ):
        """Initialize MSAK publisher transport.
        
        Args:
            topic: Kafka topic name to publish to.
            msak_config: MSAK cluster configuration.
            batch_settings: Batching configuration.
            client_options: Additional client options.
            
        Raises:
            ImportError: If google-cloud-managed-kafka is not available.
            ValueError: If configuration is invalid.
        """
        if not MSAK_AVAILABLE:
            raise ImportError(
                "confluent-kafka and google-auth are required for MSAK transport. "
                "Install with: pip install confluent-kafka google-auth"
            )
        
        self._topic = topic
        self._msak_config = msak_config
        self._batch_settings = batch_settings or BatchSettings()
        self._client_options = client_options
        
        self._client = None
        self._executor = None
        self._lock = threading.Lock()
        self._closed = False
        
        # Message batching state
        self._pending_messages: List[Dict[str, Any]] = []
        self._batch_lock = threading.Lock()
        self._last_batch_time = time.time()
        
        # Start background batch processor
        self._start_batch_processor()
    
    def _get_client(self) -> Producer:
        """Get or create the Kafka producer instance."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = self._create_producer()
        return self._client
    
    def _create_producer(self) -> Producer:
        """Create Kafka producer with OAuth authentication."""        
        config = {
            'bootstrap.servers': self._msak_config.bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'OAUTHBEARER',
            'oauth_cb': self._oauth_cb,
            'client.id': f'pubsublite-python-{threading.current_thread().ident}',
            'acks': 'all',
            'retries': 5,
            'max.in.flight.requests.per.connection': 1,
            'enable.idempotence': True,
            # SSL configuration for MSAK connections
            'ssl.endpoint.identification.algorithm': 'none',
            'ssl.ca.location': 'probe',
        }
        
        # Apply batch settings to producer config
        if self._batch_settings:
            config.update({
                'batch.size': self._batch_settings.max_bytes,
                'linger.ms': int(self._batch_settings.max_latency * 1000),
                'batch.num.messages': self._batch_settings.max_messages,
            })
        
        return Producer(config)
    
    def _oauth_cb(self, config_str):
        """OAuth callback for SASL/OAUTHBEARER authentication."""
        try:
            if self._msak_config.credentials:
                credentials = self._msak_config.credentials
            else:
                credentials, _ = default()
            
            # Apply required scopes for MSAK
            if hasattr(credentials, 'with_scopes'):
                credentials = credentials.with_scopes([
                    'https://www.googleapis.com/auth/cloud-platform'
                ])
            
            if not credentials.valid:
                request = Request()
                credentials.refresh(request)
            
            # Get actual token expiry if available
            if hasattr(credentials, 'expiry') and credentials.expiry:
                expiry_time = credentials.expiry.timestamp()
            else:
                expiry_time = time.time() + 3600  # Default 1 hour
            
            # Return token and expiry as expected by confluent-kafka
            # Format: (token_str, expiry_time[, principal, extensions])
            logger.debug(f"OAuth token refreshed, expires at: {expiry_time}")
            return credentials.token, expiry_time
            
        except Exception as e:
            logger.error(f"OAuth callback failed: {e}")
            logger.error("Please ensure you have the required MSAK permissions:")
            logger.error("  gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform")
            return None, 0
    
    def _start_batch_processor(self) -> None:
        """Start background thread for batch processing."""
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="msak-publisher")
        self._executor.submit(self._batch_processor_loop)
    
    def _batch_processor_loop(self) -> None:
        """Background loop to process message batches."""
        while not self._closed:
            try:
                self._process_pending_batch()
                time.sleep(0.01)  # 10ms check interval
            except Exception as e:
                logger.error(f"Error in batch processor: {e}")
                time.sleep(0.1)  # Longer sleep on error
    
    def _process_pending_batch(self) -> None:
        """Process pending messages if batch conditions are met."""
        should_flush = False
        messages_to_send = []
        
        with self._batch_lock:
            if not self._pending_messages:
                return
            
            # Check batch conditions
            current_time = time.time()
            time_elapsed = current_time - self._last_batch_time
            
            should_flush = (
                len(self._pending_messages) >= self._batch_settings.max_messages or
                sum(len(msg['data']) for msg in self._pending_messages) >= self._batch_settings.max_bytes or
                time_elapsed >= self._batch_settings.max_latency
            )
            
            if should_flush:
                messages_to_send = self._pending_messages.copy()
                self._pending_messages.clear()
                self._last_batch_time = current_time
        
        if should_flush and messages_to_send:
            self._send_batch(messages_to_send)
    
    def _send_batch(self, messages: List[Dict[str, Any]]) -> None:
        """Send a batch of messages to MSAK using confluent-kafka."""
        producer = self._get_client()
        
        def delivery_callback(err, msg, future):
            """Callback for message delivery."""
            if err is not None:
                future.set_exception(Exception(f"Message delivery failed: {err}"))
            else:
                message_id = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
                future.set_result(message_id)
        
        try:
            # Send messages using confluent-kafka producer
            for msg_data in messages:
                future = msg_data['future']
                
                # Create delivery callback with future binding
                def make_callback(f):
                    return lambda err, msg: delivery_callback(err, msg, f)
                
                try:
                    # Produce message to Kafka
                    headers_dict = msg_data.get('headers')
                    if headers_dict:
                        # Convert string headers to bytes as required by confluent-kafka
                        headers_list = [(k, v.encode('utf-8') if isinstance(v, str) else v) 
                                      for k, v in headers_dict.items()]
                    else:
                        headers_list = None
                    
                    # Handle partition - only pass if it's a valid non-negative integer
                    partition = msg_data.get('partition')
                    if partition is not None and partition >= 0:
                        partition_arg = partition
                    else:
                        partition_arg = -1  # Let Kafka choose partition
                    
                    producer.produce(
                        topic=self._topic,
                        key=msg_data.get('key'),
                        value=msg_data['data'],
                        headers=headers_list,
                        partition=partition_arg,
                        callback=make_callback(future)
                    )
                    
                except BufferError as e:
                    # Local queue is full, wait and retry
                    producer.poll(0.1)
                    future.set_exception(Exception(f"Producer queue full: {e}"))
                except Exception as e:
                    logger.error(f"Failed to produce message: {e}")
                    future.set_exception(e)
            
            # Trigger delivery of messages
            producer.poll(0)
            
        except Exception as e:
            logger.error(f"Batch send failed: {e}")
            # Set exception on all futures that aren't done
            for msg_data in messages:
                if not msg_data['future'].done():
                    msg_data['future'].set_exception(e)
    
    @overrides
    def publish(
        self,
        message: PubsubMessage,
        ordering_key: Optional[str] = None,
    ) -> Future:
        """Publish a message to MSAK.
        
        Args:
            message: The message to publish.
            ordering_key: Optional ordering key (used as Kafka partition key).
            
        Returns:
            Future that resolves to the message ID when published.
            
        Raises:
            RuntimeError: If transport is closed.
        """
        if self._closed:
            raise RuntimeError("Transport is closed")
        
        # Create future for result
        result_future = Future()
        
        # Convert to Kafka message format
        kafka_msg = MessageAdapter.pubsub_to_kafka(message, self._topic)
        
        # Use ordering_key if provided, otherwise use key from message
        if ordering_key:
            kafka_msg.key = ordering_key
        
        # Prepare message for batching
        msg_data = {
            'data': kafka_msg.value or b'',
            'key': kafka_msg.key,
            'headers': kafka_msg.headers or {},
            'partition': kafka_msg.partition,
            'future': result_future,
            'message_id_base': f"{self._topic}:{kafka_msg.key or ''}"
        }
        
        # Add to batch
        with self._batch_lock:
            self._pending_messages.append(msg_data)
        
        return result_future
    
    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush all pending messages.
        
        Args:
            timeout: Maximum time to wait for flush in seconds.
        """
        if self._closed:
            return
        
        start_time = time.time()
        
        # Force process any pending batch
        with self._batch_lock:
            if self._pending_messages:
                messages_to_send = self._pending_messages.copy()
                self._pending_messages.clear()
                self._last_batch_time = time.time()
        
        if 'messages_to_send' in locals() and messages_to_send:
            self._send_batch(messages_to_send)
        
        # Flush the Kafka producer
        if self._client:
            flush_timeout = timeout or 10.0
            remaining_messages = self._client.flush(timeout=flush_timeout)
            if remaining_messages > 0:
                logger.warning(f"Flush timeout: {remaining_messages} messages still pending")
    
    def close(self) -> None:
        """Close the transport and cleanup resources."""
        with self._lock:
            if self._closed:
                return
            
            self._closed = True
            
            # Flush remaining messages
            try:
                self.flush(timeout=10.0)
            except Exception as e:
                logger.warning(f"Error during flush on close: {e}")
            
            # Shutdown executor
            if self._executor:
                try:
                    self._executor.shutdown(wait=True)
                except Exception as e:
                    logger.warning(f"Error shutting down executor: {e}")
                self._executor = None
            
            # Close producer
            if self._client:
                try:
                    # Flush and close the Kafka producer
                    remaining = self._client.flush(timeout=5.0)
                    if remaining > 0:
                        logger.warning(f"Closed with {remaining} messages still pending")
                except Exception as e:
                    logger.warning(f"Error closing Kafka producer: {e}")
                self._client = None
    
    @overrides
    def __enter__(self):
        """Enter context manager."""
        return self
    
    @overrides
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.close()
    
    def __del__(self):
        """Cleanup on garbage collection."""
        try:
            self.close()
        except Exception:
            # Ignore errors during cleanup
            pass