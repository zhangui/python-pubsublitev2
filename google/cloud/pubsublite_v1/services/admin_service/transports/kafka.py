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
"""Managed Kafka transport for AdminService using Google Cloud APIs."""

import logging
from typing import Callable, Dict, Any, Optional
from google.cloud.pubsublite_v1.types import admin, common
from google.longrunning import operations_pb2
from google.protobuf.field_mask_pb2 import FieldMask
from .base import AdminServiceTransport

logger = logging.getLogger(__name__)

try:
    from google.cloud import managedkafka_v1
    from google.cloud.managedkafka_v1.types import (
        CreateTopicRequest,
        GetTopicRequest,
        ListTopicsRequest,
        UpdateTopicRequest,
        DeleteTopicRequest,
    )
except ImportError:
    managedkafka_v1 = None
    CreateTopicRequest = None
    GetTopicRequest = None
    ListTopicsRequest = None
    UpdateTopicRequest = None
    DeleteTopicRequest = None


class AdminServiceKafkaTransport(AdminServiceTransport):
    """Kafka transport for AdminService.

    This transport uses Google Cloud's managedkafka_v1 API instead of gRPC,
    allowing AdminServiceClient to manage Managed Service for Apache Kafka
    (MSAK) using the standard Google Cloud API with Application Default Credentials.
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
        """Initialize Managed Kafka admin transport.

        Args:
            admin_config: Dict with 'cluster_id' key (required for MSAK operations)
            credentials: Google Cloud credentials (uses ADC if not provided)
            **kwargs: Additional arguments passed to base transport
        """
        if managedkafka_v1 is None:
            raise ImportError(
                "google-cloud-managed-kafka is required for Managed Kafka functionality. "
                "Install it with: pip install google-cloud-managed-kafka"
            )

        # Extract cluster_id from admin_config
        self._admin_config = admin_config or {}
        self._cluster_id = self._admin_config.get('cluster_id')

        if not self._cluster_id:
            raise ValueError(
                "admin_config must contain 'cluster_id' for Managed Kafka operations. "
                "Example: admin_config={'cluster_id': 'my-cluster'}"
            )

        logger.info(f"Initializing Managed Kafka client for cluster: {self._cluster_id}")

        # Create Managed Kafka client with Google Cloud credentials
        self._managed_kafka_client = managedkafka_v1.ManagedKafkaClient(
            credentials=credentials
        )
        self._stubs = {}  # Cache: method_name -> callable

        logger.info("Managed Kafka client initialized successfully")

        # Call base __init__
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

    def _extract_project_location(self, path: str) -> tuple[str, str]:
        """Extract project and location from path.

        Args:
            path: Path like projects/{project}/locations/{location}/...

        Returns:
            Tuple of (project, location)
        """
        parts = path.split('/')
        if len(parts) >= 4 and parts[0] == 'projects' and parts[2] == 'locations':
            return parts[1], parts[3]
        raise ValueError(f"Invalid path format: {path}")

    def _build_cluster_path(self, project: str, location: str) -> str:
        """Build cluster path for Managed Kafka API."""
        return f"projects/{project}/locations/{location}/clusters/{self._cluster_id}"

    def _extract_topic_name(self, path: str) -> str:
        """Extract topic name from path: projects/*/locations/*/topics/{name}"""
        return path.split('/')[-1]

    def _managedkafka_to_pubsublite_topic(
        self,
        mk_topic: managedkafka_v1.Topic,
        location_path: str
    ) -> common.Topic:
        """Convert Managed Kafka Topic to Pub/Sub Lite Topic proto."""
        if mk_topic is None:
            raise ValueError("mk_topic is None - API returned no topic")

        logger.debug(f"Converting Managed Kafka topic: {mk_topic.name}")
        logger.debug(f"  partition_count: {mk_topic.partition_count}")
        logger.debug(f"  replication_factor: {mk_topic.replication_factor}")

        # Extract topic ID from name: projects/.../clusters/.../topics/{id}
        topic_id = mk_topic.name.split('/')[-1]

        # Get partition count
        partition_count = mk_topic.partition_count or 1

        return common.Topic(
            name=f"{location_path}/topics/{topic_id}",
            partition_config=common.Topic.PartitionConfig(
                count=partition_count,
                capacity=common.Topic.PartitionConfig.Capacity(
                    publish_mib_per_sec=4,  # Default values
                    subscribe_mib_per_sec=8,
                ),
            ),
        )

    @property
    def create_topic(
        self,
    ) -> Callable[[admin.CreateTopicRequest], common.Topic]:
        """Return callable for create_topic operation."""
        if "create_topic" not in self._stubs:
            def _create_topic(request: admin.CreateTopicRequest, **kwargs) -> common.Topic:
                """Create a Managed Kafka topic."""
                try:
                    project, location = self._extract_project_location(request.parent)

                    logger.info(f"Creating topic '{request.topic_id}' in cluster {self._cluster_id}")

                    # Extract partition count
                    num_partitions = 1
                    if request.topic and request.topic.partition_config:
                        num_partitions = request.topic.partition_config.count or 1

                    # Build paths using client helpers
                    cluster_path = self._managed_kafka_client.cluster_path(project, location, self._cluster_id)
                    topic_path = self._managed_kafka_client.topic_path(project, location, self._cluster_id, request.topic_id)

                    # Create Managed Kafka Topic object with name set
                    mk_topic = managedkafka_v1.Topic()
                    mk_topic.name = topic_path
                    mk_topic.partition_count = num_partitions
                    mk_topic.replication_factor = 3  # Default for Managed Kafka

                    # Wrap in request object
                    mk_request = CreateTopicRequest(
                        parent=cluster_path,
                        topic_id=request.topic_id,
                        topic=mk_topic,
                    )

                    # Call API with request object
                    created_topic = self._managed_kafka_client.create_topic(request=mk_request)

                    logger.info(f"Topic created: {created_topic.name if created_topic else 'None'}")

                    # Convert to Pub/Sub Lite format
                    return self._managedkafka_to_pubsublite_topic(created_topic, request.parent)

                except Exception as e:
                    logger.error(f"Error creating topic: {e}")
                    logger.error(f"Request parent: {request.parent}")
                    logger.error(f"Request topic_id: {request.topic_id}")
                    raise

            self._stubs["create_topic"] = _create_topic

        return self._stubs["create_topic"]

    @property
    def get_topic(
        self,
    ) -> Callable[[admin.GetTopicRequest], common.Topic]:
        """Return callable for get_topic operation."""
        if "get_topic" not in self._stubs:
            def _get_topic(request: admin.GetTopicRequest, **kwargs) -> common.Topic:
                """Get a Managed Kafka topic."""
                project, location = self._extract_project_location(request.name)
                topic_name = self._extract_topic_name(request.name)

                # Build topic path using client helper
                topic_path = self._managed_kafka_client.topic_path(project, location, self._cluster_id, topic_name)

                # Wrap in request object
                mk_request = GetTopicRequest(name=topic_path)

                # Get the topic
                mk_topic = self._managed_kafka_client.get_topic(request=mk_request)

                # Convert to Pub/Sub Lite format
                location_path = f"projects/{project}/locations/{location}"
                return self._managedkafka_to_pubsublite_topic(mk_topic, location_path)

            self._stubs["get_topic"] = _get_topic

        return self._stubs["get_topic"]

    @property
    def get_topic_partitions(
        self,
    ) -> Callable[[admin.GetTopicPartitionsRequest], admin.TopicPartitions]:
        """Return callable for get_topic_partitions operation."""
        if "get_topic_partitions" not in self._stubs:
            def _get_topic_partitions(request: admin.GetTopicPartitionsRequest, **kwargs) -> admin.TopicPartitions:
                """Get partition count for a Managed Kafka topic."""
                project, location = self._extract_project_location(request.name)
                topic_name = self._extract_topic_name(request.name)

                # Build topic path using client helper
                topic_path = self._managed_kafka_client.topic_path(project, location, self._cluster_id, topic_name)

                # Wrap in request object
                mk_request = GetTopicRequest(name=topic_path)

                # Get the topic
                mk_topic = self._managed_kafka_client.get_topic(request=mk_request)

                return admin.TopicPartitions(partition_count=mk_topic.partition_count)

            self._stubs["get_topic_partitions"] = _get_topic_partitions

        return self._stubs["get_topic_partitions"]

    @property
    def list_topics(
        self,
    ) -> Callable[[admin.ListTopicsRequest], admin.ListTopicsResponse]:
        """Return callable for list_topics operation."""
        if "list_topics" not in self._stubs:
            def _list_topics(request: admin.ListTopicsRequest, **kwargs) -> admin.ListTopicsResponse:
                """List Managed Kafka topics."""
                try:
                    project, location = self._extract_project_location(request.parent)

                    # Build cluster path using client helper
                    cluster_path = self._managed_kafka_client.cluster_path(project, location, self._cluster_id)

                    logger.info(f"Listing topics in cluster: {cluster_path}")

                    # Wrap in request object
                    mk_request = ListTopicsRequest(parent=cluster_path)

                    # List topics in the cluster
                    response = self._managed_kafka_client.list_topics(request=mk_request)

                    # Convert to Pub/Sub Lite format
                    topics = []
                    for i, mk_topic in enumerate(response):
                        logger.debug(f"Processing topic {i+1}: {mk_topic.name if mk_topic else 'None'}")
                        if mk_topic:
                            topics.append(
                                self._managedkafka_to_pubsublite_topic(mk_topic, request.parent)
                            )
                        else:
                            logger.warning(f"Skipping None topic at index {i}")

                    logger.info(f"Found {len(topics)} topics")
                    return admin.ListTopicsResponse(topics=topics)

                except Exception as e:
                    logger.error(f"Error listing topics: {e}")
                    logger.error(f"Request parent: {request.parent}")
                    logger.error(f"Cluster path: {cluster_path if 'cluster_path' in locals() else 'not set'}")
                    raise

            self._stubs["list_topics"] = _list_topics

        return self._stubs["list_topics"]

    @property
    def update_topic(
        self,
    ) -> Callable[[admin.UpdateTopicRequest], common.Topic]:
        """Return callable for update_topic operation."""
        if "update_topic" not in self._stubs:
            def _update_topic(request: admin.UpdateTopicRequest, **kwargs) -> common.Topic:
                """Update a Managed Kafka topic."""
                project, location = self._extract_project_location(request.topic.name)
                topic_name = self._extract_topic_name(request.topic.name)

                # Build topic path using client helper
                topic_path = self._managed_kafka_client.topic_path(project, location, self._cluster_id, topic_name)

                # Create topic object with name set
                mk_topic = managedkafka_v1.Topic()
                mk_topic.name = topic_path

                # Update partition count if requested
                update_mask = FieldMask()

                for path in request.update_mask.paths:
                    if path == "partition_config.count":
                        mk_topic.partition_count = request.topic.partition_config.count
                        update_mask.paths.append("partition_count")
                    else:
                        logger.warning(f"Update path '{path}' not supported for Managed Kafka topics")

                # Wrap in request object
                mk_request = UpdateTopicRequest(
                    update_mask=update_mask,
                    topic=mk_topic,
                )

                # Update the topic
                updated_topic = self._managed_kafka_client.update_topic(request=mk_request)

                # Convert to Pub/Sub Lite format
                location_path = f"projects/{project}/locations/{location}"
                return self._managedkafka_to_pubsublite_topic(updated_topic, location_path)

            self._stubs["update_topic"] = _update_topic

        return self._stubs["update_topic"]

    @property
    def delete_topic(
        self,
    ) -> Callable[[admin.DeleteTopicRequest], None]:
        """Return callable for delete_topic operation."""
        if "delete_topic" not in self._stubs:
            def _delete_topic(request: admin.DeleteTopicRequest, **kwargs) -> None:
                """Delete a Managed Kafka topic."""
                project, location = self._extract_project_location(request.name)
                topic_name = self._extract_topic_name(request.name)

                # Build topic path using client helper
                topic_path = self._managed_kafka_client.topic_path(project, location, self._cluster_id, topic_name)

                # Wrap in request object
                mk_request = DeleteTopicRequest(name=topic_path)

                # Delete the topic
                self._managed_kafka_client.delete_topic(request=mk_request)

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
                project, location = self._extract_project_location(request.name)
                cluster_path = self._build_cluster_path(project, location)

                # List consumer groups in the cluster
                response = self._managed_kafka_client.list_consumer_groups(parent=cluster_path)

                # Filter by topic (Managed Kafka consumer groups don't have explicit topic linkage)
                # For now, return all consumer groups
                subscriptions = []
                for consumer_group in response:
                    # Extract consumer group ID
                    group_id = consumer_group.name.split('/')[-1]
                    subscription_path = f"projects/{project}/locations/{location}/subscriptions/{group_id}"
                    subscriptions.append(subscription_path)

                return admin.ListTopicSubscriptionsResponse(subscriptions=subscriptions)

            self._stubs["list_topic_subscriptions"] = _list_topic_subscriptions

        return self._stubs["list_topic_subscriptions"]

    # Subscription operations - basic implementations
    @property
    def create_subscription(
        self,
    ) -> Callable[[admin.CreateSubscriptionRequest], common.Subscription]:
        """Return callable for create_subscription operation."""
        if "create_subscription" not in self._stubs:
            def _create_subscription(request: admin.CreateSubscriptionRequest, **kwargs):
                """Create a subscription (consumer group)."""
                project, location = self._extract_project_location(request.parent)
                cluster_path = self._build_cluster_path(project, location)

                # Create Managed Kafka consumer group
                mk_consumer_group = managedkafka_v1.ConsumerGroup()

                created_group = self._managed_kafka_client.create_consumer_group(
                    parent=cluster_path,
                    consumer_group_id=request.subscription_id,
                    consumer_group=mk_consumer_group,
                )

                # Return Subscription proto
                subscription = common.Subscription(
                    name=f"{request.parent}/subscriptions/{request.subscription_id}",
                )

                # Set topic if provided
                if request.subscription and request.subscription.topic:
                    subscription.topic = request.subscription.topic

                return subscription

            self._stubs["create_subscription"] = _create_subscription

        return self._stubs["create_subscription"]

    @property
    def get_subscription(
        self,
    ) -> Callable[[admin.GetSubscriptionRequest], common.Subscription]:
        """Return callable for get_subscription operation."""
        if "get_subscription" not in self._stubs:
            def _get_subscription(request: admin.GetSubscriptionRequest, **kwargs):
                """Get subscription (consumer group) details."""
                project, location = self._extract_project_location(request.name)
                subscription_id = self._extract_topic_name(request.name)  # Same pattern
                cluster_path = self._build_cluster_path(project, location)

                # Build consumer group path
                group_path = f"{cluster_path}/consumerGroups/{subscription_id}"

                # Get the consumer group
                consumer_group = self._managed_kafka_client.get_consumer_group(name=group_path)

                return common.Subscription(name=request.name)

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
                project, location = self._extract_project_location(request.parent)
                cluster_path = self._build_cluster_path(project, location)

                # List consumer groups
                response = self._managed_kafka_client.list_consumer_groups(parent=cluster_path)

                subscriptions = []
                for consumer_group in response:
                    group_id = consumer_group.name.split('/')[-1]
                    subscription = common.Subscription(
                        name=f"{request.parent}/subscriptions/{group_id}"
                    )
                    subscriptions.append(subscription)

                return admin.ListSubscriptionsResponse(subscriptions=subscriptions)

            self._stubs["list_subscriptions"] = _list_subscriptions

        return self._stubs["list_subscriptions"]

    @property
    def update_subscription(
        self,
    ) -> Callable[[admin.UpdateSubscriptionRequest], common.Subscription]:
        """Return callable for update_subscription operation."""
        if "update_subscription" not in self._stubs:
            def _update_subscription(request: admin.UpdateSubscriptionRequest, **kwargs):
                """Update subscription - limited support."""
                logger.warning("Subscription updates have limited support in Managed Kafka")
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
                project, location = self._extract_project_location(request.name)
                subscription_id = self._extract_topic_name(request.name)
                cluster_path = self._build_cluster_path(project, location)

                # Build consumer group path
                group_path = f"{cluster_path}/consumerGroups/{subscription_id}"

                # Delete consumer group
                self._managed_kafka_client.delete_consumer_group(name=group_path)

            self._stubs["delete_subscription"] = _delete_subscription

        return self._stubs["delete_subscription"]

    @property
    def seek_subscription(
        self,
    ) -> Callable[[admin.SeekSubscriptionRequest], operations_pb2.Operation]:
        """Return callable for seek_subscription operation."""
        if "seek_subscription" not in self._stubs:
            def _seek_subscription(request: admin.SeekSubscriptionRequest, **kwargs):
                """Seek subscription - not implemented."""
                from google.api_core.exceptions import Unimplemented
                raise Unimplemented("Seek subscription not yet implemented for Managed Kafka transport")

            self._stubs["seek_subscription"] = _seek_subscription

        return self._stubs["seek_subscription"]

    # Reservation operations - not supported in Managed Kafka
    @property
    def create_reservation(
        self,
    ) -> Callable[[admin.CreateReservationRequest], common.Reservation]:
        """Return callable for create_reservation - not supported."""
        def _not_supported(request, **kwargs):
            raise NotImplementedError(
                "Reservation operations are not supported with Managed Kafka transport. "
                "Reservations are a Pub/Sub Lite capacity management feature."
            )
        return _not_supported

    @property
    def get_reservation(
        self,
    ) -> Callable[[admin.GetReservationRequest], common.Reservation]:
        """Return callable for get_reservation - not supported."""
        return self.create_reservation

    @property
    def list_reservations(
        self,
    ) -> Callable[[admin.ListReservationsRequest], admin.ListReservationsResponse]:
        """Return callable for list_reservations - not supported."""
        return self.create_reservation

    @property
    def update_reservation(
        self,
    ) -> Callable[[admin.UpdateReservationRequest], common.Reservation]:
        """Return callable for update_reservation - not supported."""
        return self.create_reservation

    @property
    def delete_reservation(
        self,
    ) -> Callable[[admin.DeleteReservationRequest], None]:
        """Return callable for delete_reservation - not supported."""
        return self.create_reservation

    @property
    def list_reservation_topics(
        self,
    ) -> Callable[[admin.ListReservationTopicsRequest], admin.ListReservationTopicsResponse]:
        """Return callable for list_reservation_topics - not supported."""
        return self.create_reservation

    def close(self):
        """Close Managed Kafka client and release resources."""
        self._stubs.clear()

    @property
    def kind(self) -> str:
        """Return the transport kind."""
        return "kafka"

    # Operation methods (required by base)
    @property
    def list_operations(
        self,
    ) -> Callable[
        [operations_pb2.ListOperationsRequest],
        operations_pb2.ListOperationsResponse
    ]:
        """Return a callable for list_operations."""
        def _not_implemented(request):
            raise NotImplementedError("Operations not supported with Managed Kafka transport")
        return _not_implemented

    @property
    def get_operation(
        self,
    ) -> Callable[
        [operations_pb2.GetOperationRequest],
        operations_pb2.Operation
    ]:
        """Return a callable for get_operation."""
        def _not_implemented(request):
            raise NotImplementedError("Operations not supported with Managed Kafka transport")
        return _not_implemented

    @property
    def cancel_operation(
        self,
    ) -> Callable[[operations_pb2.CancelOperationRequest], None]:
        """Return a callable for cancel_operation."""
        def _not_implemented(request):
            raise NotImplementedError("Operations not supported with Managed Kafka transport")
        return _not_implemented

    @property
    def delete_operation(
        self,
    ) -> Callable[[operations_pb2.DeleteOperationRequest], None]:
        """Return a callable for delete_operation."""
        def _not_implemented(request):
            raise NotImplementedError("Operations not supported with Managed Kafka transport")
        return _not_implemented


__all__ = ("AdminServiceKafkaTransport",)
