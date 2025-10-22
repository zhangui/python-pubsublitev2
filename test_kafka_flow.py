#!/usr/bin/env python

"""
Comprehensive test script for Kafka publisher and subscriber flow.
This script helps diagnose issues with message flow between publisher and subscriber.
"""

import logging
import time
import threading
from google.cloud import pubsublite_v1

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_kafka_flow(bootstrap_servers, project_id, location, topic_name):
    """Test both publisher and subscriber with the same configuration."""

    # Try to import TokenProvider
    try:
        from samples.msak.tokenprovider import TokenProvider
        token_provider = TokenProvider()

        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': token_provider.get_token,
        }

        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': token_provider.get_token,
            'group.id': f'test-group-{int(time.time())}',  # Unique group ID
            'auto.offset.reset': 'earliest',  # Read from beginning
            'enable.auto.commit': False,
        }

        logger.info("Using OAuth authentication with TokenProvider")

    except ImportError:
        logger.warning("TokenProvider not available, using mock config for local testing")
        # Mock config for local testing (won't work with real Kafka)
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'PLAINTEXT',
        }

        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'PLAINTEXT',
            'group.id': f'test-group-{int(time.time())}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }

    # Topic and subscription paths
    topic_path = f"projects/{project_id}/locations/{location}/topics/{topic_name}"
    subscription_path = f"projects/{project_id}/locations/{location}/subscriptions/{topic_name}"

    logger.info(f"Topic path: {topic_path}")
    logger.info(f"Subscription path: {subscription_path}")
    logger.info(f"Consumer group: {consumer_config['group.id']}")

    # Test 1: Create publisher client
    logger.info("\n=== TEST 1: Creating Publisher Client ===")
    try:
        publisher = pubsublite_v1.PublisherServiceClient(
            transport="kafka",
            producer_config=producer_config,
        )
        logger.info(f"✓ Publisher created successfully")
        logger.info(f"  Transport type: {type(publisher.transport)}")
        logger.info(f"  Transport kind: {publisher.transport.kind}")
    except Exception as e:
        logger.error(f"✗ Failed to create publisher: {e}")
        return

    # Test 2: Create subscriber client
    logger.info("\n=== TEST 2: Creating Subscriber Client ===")
    try:
        subscriber = pubsublite_v1.SubscriberServiceClient(
            transport="kafka",
            consumer_config=consumer_config,
        )
        logger.info(f"✓ Subscriber created successfully")
        logger.info(f"  Transport type: {type(subscriber.transport)}")
        logger.info(f"  Transport kind: {subscriber.transport.kind}")
    except Exception as e:
        logger.error(f"✗ Failed to create subscriber: {e}")
        return

    # Test 3: Start subscriber in background thread
    logger.info("\n=== TEST 3: Starting Subscriber ===")

    messages_received = []
    subscriber_error = None
    stop_subscriber = threading.Event()

    def run_subscriber():
        nonlocal subscriber_error
        try:
            def request_generator():
                # Initial request
                logger.info("Subscriber: Sending initial request")
                yield pubsublite_v1.SubscribeRequest(
                    initial=pubsublite_v1.InitialSubscribeRequest(
                        subscription=subscription_path,
                        partition=0,
                    )
                )

                # Flow control requests
                while not stop_subscriber.is_set():
                    logger.info("Subscriber: Sending flow control request")
                    yield pubsublite_v1.SubscribeRequest(
                        flow_control=pubsublite_v1.FlowControlRequest(
                            allowed_messages=100,
                            allowed_bytes=10 * 1024 * 1024,
                        )
                    )
                    time.sleep(1)

            stream = subscriber.subscribe(requests=request_generator())

            for response in stream:
                if response.initial:
                    logger.info(f"Subscriber: Got initial response - cursor: {response.initial.cursor}")

                elif response.messages:
                    for msg in response.messages.messages:
                        data = msg.message.data.decode('utf-8', errors='ignore')
                        logger.info(f"Subscriber: Received message - offset={msg.cursor.offset}, data={data}")
                        messages_received.append(data)

                if stop_subscriber.is_set():
                    break

        except Exception as e:
            subscriber_error = e
            logger.error(f"Subscriber error: {e}")

    subscriber_thread = threading.Thread(target=run_subscriber)
    subscriber_thread.start()

    # Give subscriber time to initialize
    time.sleep(2)

    # Test 4: Publish messages
    logger.info("\n=== TEST 4: Publishing Messages ===")

    try:
        def publish_request_generator():
            # Initial request
            logger.info("Publisher: Sending initial request")
            yield pubsublite_v1.PublishRequest(
                initial_request=pubsublite_v1.InitialPublishRequest(topic=topic_path)
            )

            # Publish test messages
            for i in range(5):
                message = pubsublite_v1.PubSubMessage(
                    data=f"Test message {i}".encode('utf-8'),
                    key=f"key-{i}".encode('utf-8'),
                )
                logger.info(f"Publisher: Publishing message {i}")
                yield pubsublite_v1.PublishRequest(
                    message_publish_request=pubsublite_v1.MessagePublishRequest(
                        messages=[message]
                    )
                )
                time.sleep(0.5)  # Small delay between messages

        publish_stream = publisher.publish(requests=publish_request_generator())

        for i, response in enumerate(publish_stream):
            logger.info(f"Publisher: Got response {i}: {response}")
            if i >= 4:  # Stop after 5 responses (initial + 4 messages)
                break

        logger.info("✓ Published 5 messages successfully")

    except Exception as e:
        logger.error(f"✗ Publishing failed: {e}")

    # Wait for messages to be consumed
    logger.info("\n=== TEST 5: Waiting for Messages ===")
    time.sleep(5)

    # Stop subscriber
    stop_subscriber.set()
    subscriber_thread.join(timeout=5)

    # Check results
    logger.info("\n=== RESULTS ===")
    if subscriber_error:
        logger.error(f"Subscriber encountered error: {subscriber_error}")

    if messages_received:
        logger.info(f"✓ Received {len(messages_received)} messages:")
        for msg in messages_received:
            logger.info(f"  - {msg}")
    else:
        logger.warning("✗ No messages received")
        logger.info("\nPossible causes:")
        logger.info("1. Topic name mismatch between publisher and subscriber")
        logger.info("2. Consumer group has already consumed these messages (try new group ID)")
        logger.info("3. Network/authentication issues with Kafka broker")
        logger.info("4. Partition assignment issues")
        logger.info("\nDebug suggestions:")
        logger.info("1. Check Kafka broker logs")
        logger.info("2. Use a Kafka tool to verify messages in topic")
        logger.info("3. Try with a fresh consumer group")
        logger.info("4. Verify OAuth token is valid")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test Kafka publisher/subscriber flow")
    parser.add_argument(
        "--bootstrap-servers",
        default="bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument("--topic", default="testtopic", help="Topic name")
    parser.add_argument("--project-id", default="ygnahz-eg-codelab", help="Google Cloud project ID")
    parser.add_argument("--location", default="us-central1", help="Cloud location")

    args = parser.parse_args()

    test_kafka_flow(
        bootstrap_servers=args.bootstrap_servers,
        project_id=args.project_id,
        location=args.location,
        topic_name=args.topic,
    )