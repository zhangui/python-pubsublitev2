# -*- coding: utf-8 -*-
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
#
"""Kafka transport for SubscriberService."""

import asyncio
import logging
from typing import Callable, Iterator, Dict, Any, Optional
from google.cloud.pubsublite_v1.types import subscriber, common
from google.protobuf.timestamp_pb2 import Timestamp
from google.longrunning import operations_pb2
from .base import SubscriberServiceTransport

logger = logging.getLogger(__name__)


class SubscriberServiceKafkaTransport(SubscriberServiceTransport):
    """Kafka transport for SubscriberService.

    This transport uses Kafka as the backend instead of gRPC,
    allowing SubscriberServiceClient to consume from Managed Service
    for Apache Kafka (MSAK) using the same API.
    """

    def __init__(
        self,
        *,
        host: str = "pubsublite.googleapis.com",
        credentials=None,
        credentials_file=None,
        scopes=None,
        client_cert_source_for_mtls=None,
        quota_project_id=None,
        client_info=None,
        always_use_jwt_access=True,
        api_audience=None,
        consumer_config: Dict[str, Any] = None,
        **kwargs
    ) -> None:
        """Initialize Kafka transport.

        Args:
            consumer_config: Kafka consumer configuration dict
            **kwargs: Additional arguments passed to base transport
        """
        # Skip credential loading (Kafka handles auth via consumer_config)
        self._ignore_credentials = True

        # Store consumer_config before calling super (base doesn't accept it)
        self._consumer_config = consumer_config
        self._subscribers = {}  # Cache: subscription:partition -> AsyncKafkaSubscriber
        self._stubs = {}  # Cache: method_name -> callable (like grpc transport)

        # Call base __init__ without consumer_config
        super().__init__(
            host=host,
            credentials=credentials,
            credentials_file=credentials_file,
            scopes=scopes,
            quota_project_id=quota_project_id,
            client_info=client_info,
            always_use_jwt_access=always_use_jwt_access,
            api_audience=api_audience,
            **kwargs
        )

        # Initialize wrapped methods (required by client)
        self._prep_wrapped_messages(client_info)

    @property
    def subscribe(
        self,
    ) -> Callable[
        [Iterator[subscriber.SubscribeRequest]],
        Iterator[subscriber.SubscribeResponse]
    ]:
        """Return callable for the subscribe method over Kafka.

        Handles bidirectional streaming:
        - Receives SubscribeRequests (initial, flow_control, seek)
        - Yields SubscribeResponses (initial, messages, seek)

        Returns:
            Callable that accepts request iterator and yields responses.
        """
        # Cache the subscribe callable (required for _wrapped_methods to work)
        if "subscribe" not in self._stubs:
            def _subscribe_stream(
                request_iterator: Iterator[subscriber.SubscribeRequest],
                **kwargs  # Accept and ignore gRPC-specific kwargs
            ) -> Iterator[subscriber.SubscribeResponse]:
                """Handle bidirectional subscribe stream."""
                return self._handle_subscribe_stream(request_iterator)

            self._stubs["subscribe"] = _subscribe_stream

        return self._stubs["subscribe"]

    def _handle_subscribe_stream(
        self,
        request_iterator: Iterator[subscriber.SubscribeRequest]
    ) -> Iterator[subscriber.SubscribeResponse]:
        """Core logic for handling subscribe requests and yielding responses.

        Args:
            request_iterator: Iterator of SubscribeRequest messages

        Yields:
            SubscribeResponse messages containing messages from Kafka
        """
        kafka_sub = None
        subscription_name = None
        partition = None
        flow_tokens_messages = 0
        flow_tokens_bytes = 0

        # Create async event loop for Kafka operations
        loop = asyncio.new_event_loop()

        try:
            for request in request_iterator:
                # Handle initial request
                if request.initial:
                    subscription_name = request.initial.subscription
                    partition = request.initial.partition

                    # Extract topic name from subscription path
                    # Format: projects/*/locations/*/subscriptions/{name}
                    topic_name = subscription_name.split('/')[-1]

                    # Create cache key for this subscription:partition pair
                    cache_key = f"{subscription_name}:{partition}"

                    # Get or create subscriber for this subscription/partition
                    if cache_key not in self._subscribers:
                        from google.cloud.pubsublite.cloudpubsub.internal.async_kafka_subscriber import (
                            AsyncKafkaSubscriber,
                        )
                        from google.cloud.pubsublite.types import FlowControlSettings

                        # Create subscriber with consumer config
                        kafka_sub = AsyncKafkaSubscriber(
                            kafka_config=self._consumer_config,
                            topic_name=topic_name,
                            consumer_group=f"pubsublite-{topic_name}-p{partition}",
                            flow_control_settings=FlowControlSettings(
                                messages_outstanding=1000,
                                bytes_outstanding=10 * 1024 * 1024,  # 10MB
                            ),
                        )

                        # Start the consumer
                        loop.run_until_complete(kafka_sub.__aenter__())
                        self._subscribers[cache_key] = kafka_sub
                    else:
                        kafka_sub = self._subscribers[cache_key]

                    # Send initial response with cursor
                    # For Kafka, we'll use the current position as the cursor
                    yield subscriber.SubscribeResponse(
                        initial=subscriber.InitialSubscribeResponse(
                            cursor=common.Cursor(offset=0)  # Will be updated on first read
                        )
                    )

                # Handle flow control requests
                elif request.flow_control:
                    flow_tokens_messages += request.flow_control.allowed_messages
                    flow_tokens_bytes += request.flow_control.allowed_bytes

                    # If we have tokens and a subscriber, try to read messages
                    if kafka_sub and flow_tokens_messages > 0:
                        messages = loop.run_until_complete(kafka_sub.read())

                        if messages:
                            # Convert messages to SequencedMessage format
                            sequenced_messages = []
                            total_bytes = 0

                            for msg in messages[:flow_tokens_messages]:
                                # Convert to SequencedMessage
                                seq_msg = self._kafka_message_to_sequenced(msg)
                                if seq_msg:
                                    sequenced_messages.append(seq_msg)
                                    total_bytes += seq_msg.size_bytes

                                    # Check byte limit
                                    if total_bytes >= flow_tokens_bytes:
                                        break

                            if sequenced_messages:
                                # Update flow tokens
                                flow_tokens_messages -= len(sequenced_messages)
                                flow_tokens_bytes -= total_bytes

                                # Yield message response
                                yield subscriber.SubscribeResponse(
                                    messages=subscriber.MessageResponse(
                                        messages=sequenced_messages
                                    )
                                )

                # Handle seek requests (not implemented for Kafka yet)
                elif request.seek:
                    yield subscriber.SubscribeResponse(
                        seek=subscriber.SeekResponse(
                            cursor=common.Cursor(offset=0)
                        )
                    )

        finally:
            # Clean up the event loop
            loop.close()

    def _kafka_message_to_sequenced(self, message) -> Optional[common.SequencedMessage]:
        """Convert a Kafka message (wrapped as Cloud Pub/Sub Message) to SequencedMessage proto.

        Args:
            message: A Cloud Pub/Sub Message object wrapping Kafka data

        Returns:
            SequencedMessage proto or None if conversion fails
        """
        try:
            # Extract Kafka offset from ack_id if available
            # Format: AckId(generation=0, offset=X) encoded as "0,X"
            offset = 0
            if hasattr(message, 'ack_id'):
                if isinstance(message.ack_id, str) and ',' in message.ack_id:
                    _, offset_str = message.ack_id.split(',')
                    offset = int(offset_str)
                else:
                    # ack_id might be an AckId namedtuple
                    if hasattr(message.ack_id, 'offset'):
                        offset = message.ack_id.offset

            # Create PubSubMessage
            pubsub_message = common.PubSubMessage(
                data=message.data,
                key=message.ordering_key.encode('utf-8') if message.ordering_key else b"",
            )

            # Set attributes - need to convert to AttributeValues format
            # PubSubMessage expects attributes as map<string, AttributeValues>
            # where AttributeValues contains a list of bytes
            if message.attributes:
                for key, value in message.attributes.items():
                    # Create AttributeValues message with single value
                    attr_values = common.AttributeValues()
                    # Convert string value to bytes and add to values list
                    attr_values.values.append(value.encode('utf-8') if isinstance(value, str) else value)
                    pubsub_message.attributes[key] = attr_values

            # Calculate size
            size_bytes = len(message.data) + sum(
                len(k) + len(v) for k, v in (message.attributes or {}).items()
            )

            # Create SequencedMessage
            return common.SequencedMessage(
                cursor=common.Cursor(offset=offset),
                publish_time=message.publish_time,
                message=pubsub_message,
                size_bytes=size_bytes,
            )

        except Exception as e:
            logger.error(f"Error converting Kafka message to SequencedMessage: {e}")
            return None

    def close(self):
        """Close all Kafka subscribers and release resources."""
        loop = asyncio.new_event_loop()
        try:
            for kafka_sub in self._subscribers.values():
                loop.run_until_complete(kafka_sub.__aexit__(None, None, None))
        finally:
            loop.close()
        self._subscribers.clear()

    @property
    def kind(self) -> str:
        """Return the transport kind."""
        return "kafka"

    # Dummy implementations for operation methods (not used with Kafka)
    @property
    def list_operations(
        self,
    ) -> Callable[
        [operations_pb2.ListOperationsRequest],
        operations_pb2.ListOperationsResponse
    ]:
        """Return a callable for list_operations (not supported for Kafka)."""
        def _not_implemented(request):
            raise NotImplementedError("Operations not supported with Kafka transport")
        return _not_implemented

    @property
    def get_operation(
        self,
    ) -> Callable[
        [operations_pb2.GetOperationRequest],
        operations_pb2.Operation
    ]:
        """Return a callable for get_operation (not supported for Kafka)."""
        def _not_implemented(request):
            raise NotImplementedError("Operations not supported with Kafka transport")
        return _not_implemented

    @property
    def cancel_operation(
        self,
    ) -> Callable[[operations_pb2.CancelOperationRequest], None]:
        """Return a callable for cancel_operation (not supported for Kafka)."""
        def _not_implemented(request):
            raise NotImplementedError("Operations not supported with Kafka transport")
        return _not_implemented

    @property
    def delete_operation(
        self,
    ) -> Callable[[operations_pb2.DeleteOperationRequest], None]:
        """Return a callable for delete_operation (not supported for Kafka)."""
        def _not_implemented(request):
            raise NotImplementedError("Operations not supported with Kafka transport")
        return _not_implemented


__all__ = ("SubscriberServiceKafkaTransport",)