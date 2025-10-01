import sys
import importlib

# Force reload of the module
if 'google.cloud.pubsublite_v1' in sys.modules:
    del sys.modules['google.cloud.pubsublite_v1']
if 'google.cloud.pubsublite_v1.services.publisher_service.client' in sys.modules:
    del sys.modules['google.cloud.pubsublite_v1.services.publisher_service.client']

from google.cloud import pubsublite_v1

producer_config = {
    'bootstrap.servers': 'test:9092',
}

client = pubsublite_v1.PublisherServiceClient(
    transport='kafka',
    producer_config=producer_config,
)
print('SUCCESS: Client created')
