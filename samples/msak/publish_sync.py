#!/usr/bin/env python

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
Publish messages to Managed Service for Apache Kafka (MSAK) using PublisherServiceClient.

This sample demonstrates publishing to Google Cloud's Managed Service for Apache Kafka
using the Pub/Sub Lite v1 PublisherServiceClient with Kafka transport and TokenProvider
for OAuth authentication.
"""

from google.cloud import pubsublite_v1

try:
    from tokenprovider import TokenProvider
except ImportError:
    TokenProvider = None


def publish_messages_kafka(
    bootstrap_servers: str,
    topic_name: str,
    project_id: str,
    location: str,
    num_messages: int = 10,
):
    """Publish messages to Kafka using PublisherServiceClient with Kafka transport.

    Args:
        bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
        topic_name: Kafka topic name
        project_id: Google Cloud project ID
        location: Cloud location
        num_messages: Number of messages to publish
    """
    if TokenProvider is None:
        raise ImportError("TokenProvider not available")

    # Create Kafka config with OAuth authentication
    token_provider = TokenProvider()
    producer_config={
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
    }

    # Create client with Kafka transport
    client = pubsublite_v1.PublisherServiceClient(
        # credentials=token_provider.get_credentials(),
        transport="kafka",
        producer_config=producer_config,
    )

    # Construct topic path
    topic_path = f"projects/{project_id}/locations/{location}/topics/{topic_name}"

    def request_generator():
        # Send initial request with topic
        yield pubsublite_v1.PublishRequest(
            initial_request=pubsublite_v1.InitialPublishRequest(topic=topic_path)
        )

        # Send messages
        for i in range(num_messages):
            message = pubsublite_v1.PubSubMessage(
                data=f"Message {i}".encode('utf-8'),
                key=f"key-{i % 3}".encode('utf-8'),
            )
            yield pubsublite_v1.PublishRequest(
                message_publish_request=pubsublite_v1.MessagePublishRequest(
                    messages=[message]
                )
            )

    # Publish and process responses
    stream = client.publish(requests=request_generator())

    for i, response in enumerate(stream):
        print(f"Published message {i}: offset=fff")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Publish to MSAK using Kafka transport")
    parser.add_argument("--bootstrap-servers", default="bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="testtopic", help="Kafka topic name")
    parser.add_argument("--project-id", default="ygnahz-eg-codelab", help="Google Cloud project ID")
    parser.add_argument("--location", default="us-central1", help="Cloud location")
    parser.add_argument("--num-messages", type=int, default=10, help="Number of messages")

    args = parser.parse_args()

    publish_messages_kafka(
        bootstrap_servers=args.bootstrap_servers,
        topic_name=args.topic,
        project_id=args.project_id,
        location=args.location,
        num_messages=args.num_messages,
    )
