#!/usr/bin/env python
"""
Test AdminClient with EXACT same config as working consumer.py
"""

from confluent_kafka.admin import AdminClient
from tokenprovider import TokenProvider

# Use EXACT same config as consumer.py lines 21-32
token_provider = TokenProvider()

config = {
    'bootstrap.servers': 'bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': token_provider.get_token,
}

print("Creating AdminClient with EXACT same config as consumer.py...")
print(f"Config keys: {list(config.keys())}")

admin = AdminClient(config)
print("AdminClient created")

print("\nCalling list_topics(timeout=30)...")
try:
    metadata = admin.list_topics(timeout=30)
    print(f"✓ Success! Found {len(metadata.topics)} topics")

    for topic_name, topic_metadata in metadata.topics.items():
        if not topic_name.startswith('_'):
            print(f"  - {topic_name}: {len(topic_metadata.partitions)} partitions")

except Exception as e:
    print(f"✗ Failed: {e}")
    import traceback
    traceback.print_exc()
