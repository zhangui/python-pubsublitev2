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
"""Kafka transport for AdminService."""

import logging
from typing import Callable, Dict, Any, Optional
from google.cloud.pubsublite_v1.types import admin, common
from google.longrunning import operations_pb2
from google.protobuf.field_mask_pb2 import FieldMask
from .base import AdminServiceTransport

logger = logging.getLogger(__name__)

try:
    from confluent_kafka.admin import (
        AdminClient,
        NewTopic,
        ConfigResource,
        ResourceType,
    )
except ImportError:
    AdminClient = None
    NewTopic = None
    ConfigResource = None
    ResourceType = None


class AdminServiceKafkaTransport(AdminServiceTransport):
    """Kafka transport for AdminService.

    This transport uses Kafka AdminClient as the backend instead of gRPC,
    allowing AdminServiceClient to manage Managed Service for Apache Kafka
    (MSAK) topics and consumer groups using the same API.
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
        admin_config: Dict[str, Any] = None,
        **kwargs
    ) -> None:
        """Initialize Kafka admin transport.

        Args:
            admin_config: Kafka admin configuration dict
            **kwargs: Additional arguments passed to base transport
        """
        if AdminClient is None:
            raise ImportError(
                "confluent-kafka is required for Kafka functionality. "
                "Install it with: pip install google-cloud-pubsublite[kafka]"
            )

        # Skip credential loading (Kafka handles auth via admin_config)
        self._ignore_credentials = True

        # Store admin_config and create Kafka AdminClient
        self._admin_config = admin_config or {}
        self._admin_client = AdminClient(self._admin_config)
        self._stubs = {}  # Cache: method_name -> callable

        # Call base __init__ without admin_config
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

    def _extract_topic_name(self, path: str) -> str:
        """Extract topic name from path: projects/*/locations/*/topics/{name}"""
        return path.split('/')[-1]

    def _extract_location_path(self, path: str) -> str:
        """Extract location path from full path."""
        parts = path.split('/')
        # projects/{project}/locations/{location}
        if len(parts) >= 4:
            return '/'.join(parts[:4])
        return path

    def _build_topic_path(self, location_path: str, topic_name: str) -> str:
        """Build full topic path from location and topic name."""
        return f"{location_path}/topics/{topic_name}"

    def _kafka_to_pubsublite_topic(self, topic_name: str, topic_metadata, location_path: str) -> admin.Topic:
        """Convert Kafka topic metadata to Pub/Sub Lite Topic proto."""
        num_partitions = len(topic_metadata.partitions)

        return admin.Topic(
            name=self._build_topic_path(location_path, topic_name),
            partition_config=admin.Topic.PartitionConfig(
                count=num_partitions,
                capacity=admin.Topic.PartitionConfig.Capacity(
                    publish_mib_per_sec=4,  # Default value
                    subscribe_mib_per_sec=8,  # Default value
                ),
            ),
        )

    @property
    def create_topic(
        self,
    ) -> Callable[[admin.CreateTopicRequest], admin.Topic]:
        """Return callable for create_topic operation."""
        if "create_topic" not in self._stubs:
            def _create_topic(request: admin.CreateTopicRequest, **kwargs) -> admin.Topic:
                """Create a Kafka topic."""
                topic_name = request.topic_id
                location_path = request.parent

                # Extract partition count from request
                num_partitions = 1
                if request.topic.partition_config:
                    num_partitions = request.topic.partition_config.count or 1

                # Create NewTopic object
                new_topic = NewTopic(
                    topic_name,
                    num_partitions=num_partitions,
                    replication_factor=3,  # Default for managed Kafka
                )

                # Create topic
                fs = self._admin_client.create_topics([new_topic])

                # Wait for result
                fs[topic_name].result()

                # Return Topic proto
                return admin.Topic(
                    name=self._build_topic_path(location_path, topic_name),
                    partition_config=admin.Topic.PartitionConfig(
                        count=num_partitions,
                        capacity=admin.Topic.PartitionConfig.Capacity(
                            publish_mib_per_sec=4,
                            subscribe_mib_per_sec=8,
                        ),
                    ),
                )

            self._stubs["create_topic"] = _create_topic

        return self._stubs["create_topic"]

    @property
    def get_topic(
        self,
    ) -> Callable[[admin.GetTopicRequest], admin.Topic]:
        """Return callable for get_topic operation."""
        if "get_topic" not in self._stubs:
            def _get_topic(request: admin.GetTopicRequest, **kwargs) -> admin.Topic:
                """Get a Kafka topic."""
                topic_name = self._extract_topic_name(request.name)
                location_path = self._extract_location_path(request.name)

                # Get topic metadata
                metadata = self._admin_client.list_topics(topic=topic_name, timeout=10)

                if topic_name not in metadata.topics:
                    from google.api_core.exceptions import NotFound
                    raise NotFound(f"Topic not found: {topic_name}")

                topic_metadata = metadata.topics[topic_name]
                return self._kafka_to_pubsublite_topic(topic_name, topic_metadata, location_path)

            self._stubs["get_topic"] = _get_topic

        return self._stubs["get_topic"]

    @property
    def get_topic_partitions(
        self,
    ) -> Callable[[admin.GetTopicPartitionsRequest], admin.TopicPartitions]:
        """Return callable for get_topic_partitions operation."""
        if "get_topic_partitions" not in self._stubs:
            def _get_topic_partitions(request: admin.GetTopicPartitionsRequest, **kwargs) -> admin.TopicPartitions:
                """Get partition count for a Kafka topic."""
                topic_name = self._extract_topic_name(request.name)

                # Get topic metadata
                metadata = self._admin_client.list_topics(topic=topic_name, timeout=10)

                if topic_name not in metadata.topics:
                    from google.api_core.exceptions import NotFound
                    raise NotFound(f"Topic not found: {topic_name}")

                partition_count = len(metadata.topics[topic_name].partitions)

                return admin.TopicPartitions(partition_count=partition_count)

            self._stubs["get_topic_partitions"] = _get_topic_partitions

        return self._stubs["get_topic_partitions"]

    @property
    def list_topics(
        self,
    ) -> Callable[[admin.ListTopicsRequest], admin.ListTopicsResponse]:
        """Return callable for list_topics operation."""
        if "list_topics" not in self._stubs:
            def _list_topics(request: admin.ListTopicsRequest, **kwargs) -> admin.ListTopicsResponse:
                """List Kafka topics."""
                location_path = request.parent

                # Get all topics
                metadata = self._admin_client.list_topics(timeout=10)

                # Convert to Topic protos, filtering out internal topics
                topics = []
                for topic_name, topic_metadata in metadata.topics.items():
                    # Skip internal topics (starting with _ or __consumer_offsets)
                    if not topic_name.startswith('_'):
                        topics.append(
                            self._kafka_to_pubsublite_topic(topic_name, topic_metadata, location_path)
                        )

                return admin.ListTopicsResponse(topics=topics)

            self._stubs["list_topics"] = _list_topics

        return self._stubs["list_topics"]

    @property
    def update_topic(
        self,
    ) -> Callable[[admin.UpdateTopicRequest], admin.Topic]:
        """Return callable for update_topic operation."""
        if "update_topic" not in self._stubs:
            def _update_topic(request: admin.UpdateTopicRequest, **kwargs) -> admin.Topic:
                """Update a Kafka topic."""
                topic_name = self._extract_topic_name(request.topic.name)

                # For now, only support partition count updates
                for path in request.update_mask.paths:
                    if path == "partition_config.count":
                        from confluent_kafka.admin import NewPartitions
                        new_count = request.topic.partition_config.count

                        # Get current partition count
                        metadata = self._admin_client.list_topics(topic=topic_name, timeout=10)
                        current_count = len(metadata.topics[topic_name].partitions)

                        if new_count > current_count:
                            new_partitions = NewPartitions(topic_name, new_count)
                            fs = self._admin_client.create_partitions([new_partitions])
                            fs[topic_name].result()
                    else:
                        logger.warning(f"Update path '{path}' not supported for Kafka topics")

                return request.topic

            self._stubs["update_topic"] = _update_topic

        return self._stubs["update_topic"]

    @property
    def delete_topic(
        self,
    ) -> Callable[[admin.DeleteTopicRequest], None]:
        """Return callable for delete_topic operation."""
        if "delete_topic" not in self._stubs:
            def _delete_topic(request: admin.DeleteTopicRequest, **kwargs) -> None:
                """Delete a Kafka topic."""
                topic_name = self._extract_topic_name(request.name)

                # Delete topic
                fs = self._admin_client.delete_topics([topic_name])

                # Wait for result
                fs[topic_name].result()

            self._stubs["delete_topic"] = _delete_topic

        return self._stubs["delete_topic"]

    @property
    def list_topic_subscriptions(
        self,
    ) -> Callable[[admin.ListTopicSubscriptionsRequest], admin.ListTopicSubscriptionsResponse]:
        """Return callable for list_topic_subscriptions operation."""
        if "list_topic_subscriptions" not in self._stubs:
            def _list_topic_subscriptions(request: admin.ListTopicSubscriptionsRequest, **kwargs):
                """List subscriptions (consumer groups) for a topic."""
                # For MVP, return empty list - would need to query all consumer groups
                # and filter by topic, which is complex
                return admin.ListTopicSubscriptionsResponse(subscriptions=[])

            self._stubs["list_topic_subscriptions"] = _list_topic_subscriptions

        return self._stubs["list_topic_subscriptions"]

    # Subscription operations - basic implementations
    @property
    def create_subscription(
        self,
    ) -> Callable[[admin.CreateSubscriptionRequest], admin.Subscription]:
        """Return callable for create_subscription operation."""
        if "create_subscription" not in self._stubs:
            def _create_subscription(request: admin.CreateSubscriptionRequest, **kwargs):
                """Create a subscription (consumer group is created implicitly)."""
                subscription_id = request.subscription_id
                location_path = request.parent

                # Consumer groups are created implicitly in Kafka when first consumer joins
                # We just validate the topic exists
                topic_name = self._extract_topic_name(request.subscription.topic)

                try:
                    metadata = self._admin_client.list_topics(topic=topic_name, timeout=10)
                    if topic_name not in metadata.topics:
                        from google.api_core.exceptions import NotFound
                        raise NotFound(f"Topic not found: {topic_name}")
                except Exception:
                    from google.api_core.exceptions import NotFound
                    raise NotFound(f"Topic not found: {topic_name}")

                # Return Subscription proto
                return admin.Subscription(
                    name=f"{location_path}/subscriptions/{subscription_id}",
                    topic=request.subscription.topic,
                )

            self._stubs["create_subscription"] = _create_subscription

        return self._stubs["create_subscription"]

    @property
    def get_subscription(
        self,
    ) -> Callable[[admin.GetSubscriptionRequest], admin.Subscription]:
        """Return callable for get_subscription operation."""
        if "get_subscription" not in self._stubs:
            def _get_subscription(request: admin.GetSubscriptionRequest, **kwargs):
                """Get subscription (consumer group) details."""
                # For MVP, return minimal subscription info
                # Would need to use describe_consumer_groups() for full implementation
                return admin.Subscription(name=request.name)

            self._stubs["get_subscription"] = _get_subscription

        return self._stubs["get_subscription"]

    @property
    def list_subscriptions(
        self,
    ) -> Callable[[admin.ListSubscriptionsRequest], admin.ListSubscriptionsResponse]:
        """Return callable for list_subscriptions operation."""
        if "list_subscriptions" not in self._stubs:
            def _list_subscriptions(request: admin.ListSubscriptionsRequest, **kwargs):
                """List subscriptions (consumer groups)."""
                # For MVP, return empty list - would need list_consumer_groups()
                return admin.ListSubscriptionsResponse(subscriptions=[])

            self._stubs["list_subscriptions"] = _list_subscriptions

        return self._stubs["list_subscriptions"]

    @property
    def update_subscription(
        self,
    ) -> Callable[[admin.UpdateSubscriptionRequest], admin.Subscription]:
        """Return callable for update_subscription operation."""
        if "update_subscription" not in self._stubs:
            def _update_subscription(request: admin.UpdateSubscriptionRequest, **kwargs):
                """Update subscription - limited support in Kafka."""
                logger.warning("Subscription updates have limited support in Kafka")
                return request.subscription

            self._stubs["update_subscription"] = _update_subscription

        return self._stubs["update_subscription"]

    @property
    def delete_subscription(
        self,
    ) -> Callable[[admin.DeleteSubscriptionRequest], None]:
        """Return callable for delete_subscription operation."""
        if "delete_subscription" not in self._stubs:
            def _delete_subscription(request: admin.DeleteSubscriptionRequest, **kwargs):
                """Delete a subscription (consumer group)."""
                subscription_name = self._extract_topic_name(request.name)  # Extract subscription ID

                # Delete consumer group
                fs = self._admin_client.delete_consumer_groups([subscription_name])
                fs[subscription_name].result()

            self._stubs["delete_subscription"] = _delete_subscription

        return self._stubs["delete_subscription"]

    @property
    def seek_subscription(
        self,
    ) -> Callable[[admin.SeekSubscriptionRequest], operations_pb2.Operation]:
        """Return callable for seek_subscription operation."""
        if "seek_subscription" not in self._stubs:
            def _seek_subscription(request: admin.SeekSubscriptionRequest, **kwargs):
                """Seek subscription - not fully implemented in MVP."""
                from google.api_core.exceptions import Unimplemented
                raise Unimplemented("Seek subscription not yet implemented for Kafka transport")

            self._stubs["seek_subscription"] = _seek_subscription

        return self._stubs["seek_subscription"]

    # Reservation operations - not supported in Kafka
    @property
    def create_reservation(
        self,
    ) -> Callable[[admin.CreateReservationRequest], admin.Reservation]:
        """Return callable for create_reservation - not supported."""
        def _not_supported(request, **kwargs):
            raise NotImplementedError(
                "Reservation operations are not supported with Kafka transport. "
                "Reservations are a Pub/Sub Lite capacity management feature."
            )
        return _not_supported

    @property
    def get_reservation(
        self,
    ) -> Callable[[admin.GetReservationRequest], admin.Reservation]:
        """Return callable for get_reservation - not supported."""
        return self.create_reservation  # Same not supported handler

    @property
    def list_reservations(
        self,
    ) -> Callable[[admin.ListReservationsRequest], admin.ListReservationsResponse]:
        """Return callable for list_reservations - not supported."""
        return self.create_reservation  # Same not supported handler

    @property
    def update_reservation(
        self,
    ) -> Callable[[admin.UpdateReservationRequest], admin.Reservation]:
        """Return callable for update_reservation - not supported."""
        return self.create_reservation  # Same not supported handler

    @property
    def delete_reservation(
        self,
    ) -> Callable[[admin.DeleteReservationRequest], None]:
        """Return callable for delete_reservation - not supported."""
        return self.create_reservation  # Same not supported handler

    @property
    def list_reservation_topics(
        self,
    ) -> Callable[[admin.ListReservationTopicsRequest], admin.ListReservationTopicsResponse]:
        """Return callable for list_reservation_topics - not supported."""
        return self.create_reservation  # Same not supported handler

    def close(self):
        """Close Kafka admin client and release resources."""
        # Kafka AdminClient doesn't have explicit close, but we clear references
        self._stubs.clear()

    @property
    def kind(self) -> str:
        """Return the transport kind."""
        return "kafka"

    # Operation methods (required by base, not used with Kafka)
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


__all__ = ("AdminServiceKafkaTransport",)
