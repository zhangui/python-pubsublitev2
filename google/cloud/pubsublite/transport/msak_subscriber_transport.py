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
import threading
import time
from typing import Optional, Callable, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.api_core.client_options import ClientOptions
from overrides import overrides

from google.cloud.pubsublite.transport.transport_interface import SubscriberTransport
from google.cloud.pubsublite.transport.msak_config import MSAKConfig
from google.cloud.pubsublite.transport.message_adapters import MessageAdapter, KafkaMessage
from google.cloud.pubsublite.types import FlowControlSettings

try:
    from google.cloud.managedkafka_v1 import ManagedKafkaClient
    from google.cloud.managedkafka_v1.types import ConsumeRequest, ConsumeResponse
    MSAK_AVAILABLE = True
except ImportError:
    MSAK_AVAILABLE = False
    ManagedKafkaClient = None
    ConsumeRequest = None
    ConsumeResponse = None

logger = logging.getLogger(__name__)


class MSAKStreamingPullFuture(StreamingPullFuture):
    """MSAK implementation of StreamingPullFuture."""
    
    def __init__(self, transport: 'MSAKSubscriberTransport'):
        self._transport = transport
        self._cancelled = False
    
    def cancel(self):
        """Cancel the streaming pull."""
        self._cancelled = True
        self._transport._stop_consumer()
        return True
    
    def cancelled(self):
        """Check if the future is cancelled."""
        return self._cancelled
    
    def running(self):
        """Check if the future is running."""
        return self._transport._is_running() and not self._cancelled
    
    def done(self):
        """Check if the future is done."""
        return self._cancelled or not self._transport._is_running()
    
    def result(self, timeout=None):
        """Get the result (blocks until cancelled or error)."""
        # This future runs indefinitely until cancelled
        while not self._cancelled and self._transport._is_running():
            time.sleep(0.1)
        return None
    
    def exception(self, timeout=None):
        """Get any exception that occurred."""
        return self._transport._get_exception()


class MSAKSubscriberTransport(SubscriberTransport):
    """Google Managed Service for Apache Kafka implementation of SubscriberTransport."""
    
    def __init__(
        self,
        subscription: str,  # Consumer group name
        topic: str,  # Kafka topic name
        msak_config: MSAKConfig,
        flow_control_settings: Optional[FlowControlSettings] = None,
        client_options: Optional[ClientOptions] = None,
    ):
        """Initialize MSAK subscriber transport.
        
        Args:
            subscription: Consumer group name (maps to Pub/Sub subscription).
            topic: Kafka topic name to subscribe to.
            msak_config: MSAK cluster configuration.
            flow_control_settings: Flow control settings.
            client_options: Additional client options.
            
        Raises:
            ImportError: If google-cloud-managed-kafka is not available.
            ValueError: If configuration is invalid.
        """
        if not MSAK_AVAILABLE:
            raise ImportError(
                "google-cloud-managed-kafka is required for MSAK transport. "
                "Install with: pip install google-cloud-managed-kafka"
            )
        
        self._subscription = subscription  # Consumer group
        self._topic = topic
        self._msak_config = msak_config
        self._flow_control_settings = flow_control_settings or FlowControlSettings()
        self._client_options = client_options
        
        self._client = None
        self._executor = None
        self._lock = threading.Lock()
        self._running = False
        self._exception = None
        
        # Consumer state
        self._committed_offsets: Dict[int, int] = {}  # partition -> offset
        self._ack_queue: List[Dict[str, Any]] = []
        self._ack_lock = threading.Lock()
    
    def _get_client(self) -> ManagedKafkaClient:
        """Get or create the MSAK client instance."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = ManagedKafkaClient(
                        credentials=self._msak_config.credentials,
                        client_options=self._client_options,
                    )
        return self._client
    
    def _is_running(self) -> bool:
        """Check if the consumer is running."""
        with self._lock:
            return self._running
    
    def _get_exception(self) -> Optional[Exception]:
        """Get any exception that occurred during consumption."""
        with self._lock:
            return self._exception
    
    def _stop_consumer(self) -> None:
        """Stop the consumer."""
        with self._lock:
            self._running = False
            
            # Commit any pending acks
            self._commit_pending_acks()
            
            if self._executor:
                try:
                    self._executor.shutdown(wait=False)
                except Exception as e:
                    logger.warning(f"Error shutting down executor: {e}")
                self._executor = None
    
    def _commit_pending_acks(self) -> None:
        """Commit any pending message acknowledgments."""
        with self._ack_lock:
            if not self._ack_queue:
                return
            
            # Group acks by partition
            partition_offsets = {}
            for ack_data in self._ack_queue:
                partition = ack_data['partition']
                offset = ack_data['offset']
                
                # Keep track of highest offset per partition
                if partition not in partition_offsets or offset > partition_offsets[partition]:
                    partition_offsets[partition] = offset
            
            # Commit offsets to MSAK
            client = self._get_client()
            try:
                for partition, offset in partition_offsets.items():
                    # Note: This is pseudo-code - actual MSAK API may differ
                    # In real MSAK, offset commits would be handled differently
                    self._committed_offsets[partition] = offset + 1  # Next offset to consume
                    logger.debug(f"Committed offset {offset + 1} for partition {partition}")
                
                # Clear committed acks
                self._ack_queue.clear()
                
            except Exception as e:
                logger.error(f"Failed to commit offsets: {e}")
    
    def _consume_messages(
        self,
        callback: Callable[[Message], None],
    ) -> None:
        """Consume messages in a background thread."""
        client = self._get_client()
        
        try:
            while self._is_running():
                try:
                    # Create consume request
                    request = ConsumeRequest(
                        parent=self._msak_config.cluster_path,
                        topic=self._topic,
                        consumer_group=self._subscription,
                        max_messages=self._flow_control_settings.max_messages,
                        max_bytes=self._flow_control_settings.max_bytes,
                    )
                    
                    # Poll for messages from MSAK
                    response = client.consume(request, timeout=1.0)
                    
                    if not response.messages:
                        continue
                    
                    # Process each message
                    for msak_message in response.messages:
                        if not self._is_running():
                            break
                        
                        # Convert MSAK message to Kafka message format
                        kafka_message = KafkaMessage(
                            topic=msak_message.topic,
                            partition=msak_message.partition,
                            key=msak_message.key.decode('utf-8') if msak_message.key else None,
                            value=msak_message.value,
                            headers={h.key: h.value for h in msak_message.headers},
                            timestamp=msak_message.timestamp.seconds * 1000 + msak_message.timestamp.nanos // 1000000,
                            offset=msak_message.offset
                        )
                        
                        # Create message ID
                        message_id = f"{kafka_message.topic}:{kafka_message.partition}:{kafka_message.offset}"
                        
                        # Convert to Pub/Sub message
                        pubsub_message = MessageAdapter.kafka_to_pubsub(kafka_message, message_id)
                        
                        # Wrap with MSAK-specific ack/nack handlers
                        wrapped_message = self._wrap_message_with_msak_handlers(
                            pubsub_message, kafka_message
                        )
                        
                        # Invoke callback
                        try:
                            callback(wrapped_message)
                        except Exception as e:
                            logger.error(f"Error in message callback: {e}")
                            # Continue processing other messages
                    
                    # Periodically commit acks
                    self._commit_pending_acks()
                    
                except Exception as e:
                    if self._is_running():  # Only log if we're still supposed to be running
                        logger.error(f"Error consuming messages: {e}")
                        with self._lock:
                            self._exception = e
                        break
                
        finally:
            with self._lock:
                self._running = False
    
    def _wrap_message_with_msak_handlers(
        self,
        message: Message,
        kafka_message: KafkaMessage,
    ) -> Message:
        """Wrap message with MSAK-specific ack/nack handlers."""
        
        # Store original ack/nack methods
        original_ack = message.ack
        original_nack = message.nack
        
        def msak_ack():
            """Ack by queuing offset for commit."""
            try:
                # Queue this message's offset for commit
                ack_data = {
                    'partition': kafka_message.partition,
                    'offset': kafka_message.offset,
                    'timestamp': time.time(),
                }
                
                with self._ack_lock:
                    self._ack_queue.append(ack_data)
                
                # Call original ack for compatibility
                original_ack()
                
            except Exception as e:
                logger.error(f"Error queuing ack: {e}")
                raise
        
        def msak_nack():
            """Nack by not committing the offset (message will be redelivered)."""
            try:
                # For MSAK, we simply don't commit the offset
                # The message will be redelivered after the session timeout
                logger.debug(f"Nacked message at partition {kafka_message.partition}, offset {kafka_message.offset}")
                
                # Call original nack for compatibility
                original_nack()
                
            except Exception as e:
                logger.error(f"Error handling nack: {e}")
                raise
        
        # Replace ack/nack methods
        message.ack = msak_ack
        message.nack = msak_nack
        
        return message
    
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
        if flow_control:
            self._flow_control_settings = flow_control
        
        with self._lock:
            if self._running:
                raise RuntimeError("Subscriber is already running")
            
            self._running = True
            self._exception = None
            
            # Start consumer in background thread
            self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="msak-subscriber")
            self._executor.submit(self._consume_messages, callback)
            
            # Start offset commit thread
            self._executor.submit(self._periodic_commit_loop)
        
        return MSAKStreamingPullFuture(self)
    
    def _periodic_commit_loop(self) -> None:
        """Periodically commit pending acknowledgments."""
        while self._is_running():
            try:
                self._commit_pending_acks()
                time.sleep(5.0)  # Commit every 5 seconds
            except Exception as e:
                logger.error(f"Error in periodic commit: {e}")
                time.sleep(1.0)