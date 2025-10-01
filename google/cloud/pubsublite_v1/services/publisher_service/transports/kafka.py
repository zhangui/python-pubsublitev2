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
"""Kafka transport for PublisherService."""

import asyncio
from typing import Callable, Iterator
from google.cloud.pubsublite_v1.types import publisher
from google.longrunning import operations_pb2
from google.cloud.pubsublite.cloudpubsub.internal.kafka_config import KafkaConfig
from google.cloud.pubsublite.cloudpubsub.internal.async_kafka_publisher import AsyncKafkaPublisher
from .base import PublisherServiceTransport


class PublisherServiceKafkaTransport(PublisherServiceTransport):
    """Kafka transport for PublisherService.

    This transport uses Kafka as the backend instead of gRPC,
    allowing PublisherServiceClient to publish to Managed Service
    for Apache Kafka (MSAK) using the same API.
    """

    def __init__(
        self,
        *,
        kafka_config: KafkaConfig,
        **kwargs
    ) -> None:
        """Initialize Kafka transport.

        Args:
            kafka_config: Kafka configuration with bootstrap servers and auth
            **kwargs: Additional arguments (ignored, for compatibility with base)
        """
        # Skip credential loading (Kafka handles auth via kafka_config)
        self._ignore_credentials = True
        super().__init__(**kwargs)

        self._kafka_config = kafka_config
        self._publishers = {}  # Cache: topic_name -> AsyncKafkaPublisher

    @property
    def publish(
        self,
    ) -> Callable[
        [Iterator[publisher.PublishRequest]],
        Iterator[publisher.PublishResponse]
    ]:
        """Return callable for the publish method over Kafka.

        Converts PublishRequest stream to Kafka producer calls,
        mimicking the gRPC streaming pattern.

        Returns:
            Callable that accepts request iterator and yields responses.
        """
        def _publish_stream(
            request_iterator: Iterator[publisher.PublishRequest],
        ) -> Iterator[publisher.PublishResponse]:
            """Handle publish request stream."""
            topic_name = None
            kafka_pub = None

            for request in request_iterator:
                # Handle initial request with topic
                if request.HasField('initial_request'):
                    # Extract topic name from path: projects/*/locations/*/topics/{name}
                    topic_path = request.initial_request.topic
                    topic_name = topic_path.split('/')[-1]

                    # Get or create publisher for this topic
                    if topic_name not in self._publishers:
                        kafka_pub = AsyncKafkaPublisher(self._kafka_config, topic_name)
                        asyncio.run(kafka_pub.__aenter__())
                        self._publishers[topic_name] = kafka_pub
                    else:
                        kafka_pub = self._publishers[topic_name]

                    # No response for initial request
                    continue

                # Handle message requests
                for message in request.messages:
                    # Publish to Kafka using AsyncKafkaPublisher
                    ack_id = asyncio.run(kafka_pub.publish(
                        data=message.data,
                        ordering_key=message.ordering_key,
                        **dict(message.attributes)
                    ))

                    # Extract offset from ack_id (format: "topic:partition:offset")
                    offset = int(ack_id.split(':')[-1])

                    # Yield PublishResponse
                    yield publisher.PublishResponse(
                        start_cursor=publisher.Cursor(offset=offset)
                    )

        return _publish_stream

    def close(self):
        """Close all Kafka publishers and release resources."""
        for kafka_pub in self._publishers.values():
            asyncio.run(kafka_pub.__aexit__(None, None, None))
        self._publishers.clear()

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


__all__ = ("PublisherServiceKafkaTransport",)
