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
"""Kafka transport for AdminService.

This module provides a transport layer that allows AdminServiceClient to manage
Managed Service for Apache Kafka (MSAK) topics using Google Cloud's Managed
Kafka API instead of the standard gRPC transport.
"""

import logging
from typing import Callable, Dict, Any, Optional, Tuple
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

    Manages Managed Service for Apache Kafka (MSAK) topics using Google
    Cloud's Managed Kafka API with Application Default Credentials.
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
        """Initialize Kafka transport.

        Args:
            admin_config: Dict with 'cluster_id' key (required).
            credentials: Google Cloud credentials (uses ADC if not provided).
            **kwargs: Additional arguments passed to base transport.
        """
        if managedkafka_v1 is None:
            raise ImportError(
                "google-cloud-managed-kafka is required for Kafka transport. "
                "Install with: pip install google-cloud-managed-kafka"
            )

        self._admin_config = admin_config or {}
        self._cluster_id = self._admin_config.get('cluster_id')

        if not self._cluster_id:
            raise ValueError(
                "admin_config must contain 'cluster_id'. "
                "Example: admin_config={'cluster_id': 'my-cluster'}"
            )

        self._managed_kafka_client = managedkafka_v1.ManagedKafkaClient(
            credentials=credentials
        )
        self._stubs = {}

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

        self._prep_wrapped_messages(client_info)

    def _extract_project_location(self, path: str) -> Tuple[str, str]:
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

    def _managedkafka_to_pubsublite_topic(
        self,
        mk_topic,  # Type: managedkafka_v1.Topic
        location_path: str
    ) -> common.Topic:
        """Convert Managed Kafka Topic to Pub/Sub Lite Topic proto."""
        topic_id = mk_topic.name.split('/')[-1]
        return common.Topic(
            name=f"{location_path}/topics/{topic_id}",
            partition_config=common.Topic.PartitionConfig(
                count=mk_topic.partition_count or 1,
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
                project, location = self._extract_project_location(request.parent)

                num_partitions = 1
                if request.topic and request.topic.partition_config:
                    num_partitions = request.topic.partition_config.count or 1

                cluster_path = self._managed_kafka_client.cluster_path(
                    project, location, self._cluster_id
                )
                topic_path = self._managed_kafka_client.topic_path(
                    project, location, self._cluster_id, request.topic_id
                )

                mk_topic = managedkafka_v1.Topic()
                mk_topic.name = topic_path
                mk_topic.partition_count = num_partitions
                mk_topic.replication_factor = 3  # Default for Managed Kafka

                mk_request = CreateTopicRequest(
                    parent=cluster_path,
                    topic_id=request.topic_id,
                    topic=mk_topic,
                )

                created_topic = self._managed_kafka_client.create_topic(request=mk_request)
                return self._managedkafka_to_pubsublite_topic(created_topic, request.parent)

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
                topic_name = request.name.split('/')[-1]

                topic_path = self._managed_kafka_client.topic_path(
                    project, location, self._cluster_id, topic_name
                )
                mk_request = GetTopicRequest(name=topic_path)
                mk_topic = self._managed_kafka_client.get_topic(request=mk_request)

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
                topic_name = request.name.split('/')[-1]

                topic_path = self._managed_kafka_client.topic_path(
                    project, location, self._cluster_id, topic_name
                )
                mk_request = GetTopicRequest(name=topic_path)
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
                project, location = self._extract_project_location(request.parent)

                cluster_path = self._managed_kafka_client.cluster_path(
                    project, location, self._cluster_id
                )
                mk_request = ListTopicsRequest(parent=cluster_path)
                response = self._managed_kafka_client.list_topics(request=mk_request)

                topics = [
                    self._managedkafka_to_pubsublite_topic(mk_topic, request.parent)
                    for mk_topic in response
                ]
                return admin.ListTopicsResponse(topics=topics)

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
                topic_name = request.topic.name.split('/')[-1]

                topic_path = self._managed_kafka_client.topic_path(
                    project, location, self._cluster_id, topic_name
                )

                mk_topic = managedkafka_v1.Topic()
                mk_topic.name = topic_path

                update_mask = FieldMask()
                for path in request.update_mask.paths:
                    if path == "partition_config.count":
                        mk_topic.partition_count = request.topic.partition_config.count
                        update_mask.paths.append("partition_count")
                    else:
                        logger.warning(
                            f"Update path '{path}' not supported for Kafka"
                        )

                mk_request = UpdateTopicRequest(
                    update_mask=update_mask,
                    topic=mk_topic,
                )
                updated_topic = self._managed_kafka_client.update_topic(request=mk_request)

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
                topic_name = request.name.split('/')[-1]

                topic_path = self._managed_kafka_client.topic_path(
                    project, location, self._cluster_id, topic_name
                )
                mk_request = DeleteTopicRequest(name=topic_path)
                self._managed_kafka_client.delete_topic(request=mk_request)

            self._stubs["delete_topic"] = _delete_topic

        return self._stubs["delete_topic"]

    # Subscription operations not supported - consumer groups are managed by consumers
    def _subscription_not_supported(self, operation_name: str):
        """Subscription operations not supported with Kafka transport."""
        def _not_supported(request, **kwargs):
            raise NotImplementedError(
                f"{operation_name} not supported with Kafka transport. "
                "Consumer groups are auto-created and managed by consumers at runtime."
            )
        return _not_supported

    @property
    def list_topic_subscriptions(self):
        return self._subscription_not_supported("list_topic_subscriptions")

    @property
    def create_subscription(self):
        return self._subscription_not_supported("create_subscription")

    @property
    def get_subscription(self):
        return self._subscription_not_supported("get_subscription")

    @property
    def list_subscriptions(self):
        return self._subscription_not_supported("list_subscriptions")

    @property
    def update_subscription(self):
        return self._subscription_not_supported("update_subscription")

    @property
    def delete_subscription(self):
        return self._subscription_not_supported("delete_subscription")

    @property
    def seek_subscription(self):
        return self._subscription_not_supported("seek_subscription")

    # Reservation operations - not supported in Managed Kafka
    def _reservation_not_supported(self, operation_name: str):
        """Reservation operations not supported with Kafka transport."""
        def _not_supported(request, **kwargs):
            raise NotImplementedError(
                f"{operation_name} not supported with Kafka transport. "
                "Reservations are a Pub/Sub Lite capacity management feature."
            )
        return _not_supported

    @property
    def create_reservation(self):
        return self._reservation_not_supported("create_reservation")

    @property
    def get_reservation(self):
        return self._reservation_not_supported("get_reservation")

    @property
    def list_reservations(self):
        return self._reservation_not_supported("list_reservations")

    @property
    def update_reservation(self):
        return self._reservation_not_supported("update_reservation")

    @property
    def delete_reservation(self):
        return self._reservation_not_supported("delete_reservation")

    @property
    def list_reservation_topics(self):
        return self._reservation_not_supported("list_reservation_topics")

    def close(self):
        """Close Managed Kafka client and release resources."""
        self._stubs.clear()

    @property
    def kind(self) -> str:
        """Return the transport kind."""
        return "kafka"

    # Long-running operation methods (required by base, not supported)
    def _operation_not_supported(self, operation_name: str):
        """Long-running operations not supported with Kafka transport."""
        def _not_implemented(request):
            raise NotImplementedError(
                f"{operation_name} not supported with Kafka transport"
            )
        return _not_implemented

    @property
    def list_operations(self):
        return self._operation_not_supported("list_operations")

    @property
    def get_operation(self):
        return self._operation_not_supported("get_operation")

    @property
    def cancel_operation(self):
        return self._operation_not_supported("cancel_operation")

    @property
    def delete_operation(self):
        return self._operation_not_supported("delete_operation")


__all__ = ("AdminServiceKafkaTransport",)
