#!/usr/bin/env python
"""Test Kafka transport publish with proper response structure."""

import os
os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)

from google.cloud import pubsublite_v1

producer_config = {
    'bootstrap.servers': 'localhost:9092',
}

client = pubsublite_v1.PublisherServiceClient(
    transport='kafka',
    producer_config=producer_config,
)

print('✓ Client created')

topic_path = 'projects/test/locations/us-central1/topics/test'

def request_generator():
    yield pubsublite_v1.PublishRequest(
        initial_request=pubsublite_v1.InitialPublishRequest(topic=topic_path)
    )
    yield pubsublite_v1.PublishRequest(
        message_publish_request=pubsublite_v1.MessagePublishRequest(
            messages=[pubsublite_v1.PubSubMessage(data=b'test', key=b'key1')]
        )
    )

print('✓ Request generator created')

try:
    stream = client.publish(requests=request_generator())
    print('✓ Publish stream created')

    for i, resp in enumerate(stream):
        print(f'✓ Response {i}: offset={resp.message_response.start_cursor.offset}')
        break  # Just test first response
except Exception as e:
    print(f'✗ Error: {type(e).__name__}: {e}')
    import traceback
    traceback.print_exc()
