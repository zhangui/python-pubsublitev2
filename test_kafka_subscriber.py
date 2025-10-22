#!/usr/bin/env python

"""Test Kafka subscriber with mocked credentials."""

import logging
from google.cloud import pubsublite_v1

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Mock token provider for testing
class MockTokenProvider:
    def get_token(self):
        # Return a mock token - this won't work with real Kafka but allows testing the flow
        return "mock_token", 3600

def test_kafka_subscriber():
    """Test the Kafka subscriber flow."""

    # Create mock Kafka config (won't connect to real Kafka)
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'security.protocol': 'PLAINTEXT',  # No auth for testing
    }

    # Create client with Kafka transport
    client = pubsublite_v1.SubscriberServiceClient(
        transport="kafka",
        consumer_config=producer_config,
    )

    print(f"Created SubscriberServiceClient with Kafka transport")
    print(f"Client type: {type(client)}")
    print(f"Transport type: {type(client.transport)}")
    print(f"Transport kind: {client.transport.kind}")

    # Test request generator
    def request_generator():
        # Send initial request
        print("Sending initial request")
        yield pubsublite_v1.SubscribeRequest(
            initial=pubsublite_v1.InitialSubscribeRequest(
                subscription="projects/test-project/locations/us-central1/subscriptions/testtopic",
                partition=0,
            )
        )

        # Send flow control request
        print("Sending flow control request")
        yield pubsublite_v1.SubscribeRequest(
            flow_control=pubsublite_v1.FlowControlRequest(
                allowed_messages=10,
                allowed_bytes=1024 * 1024,
            )
        )

    try:
        # Start subscription stream
        print("Starting subscription stream...")
        stream = client.subscribe(requests=request_generator())

        # Process responses
        for i, response in enumerate(stream):
            print(f"Received response {i}: {response}")
            if i > 2:  # Stop after a few responses
                break

    except Exception as e:
        print(f"Error in subscription stream: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_kafka_subscriber()