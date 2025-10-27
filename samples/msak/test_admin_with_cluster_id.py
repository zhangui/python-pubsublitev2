#!/usr/bin/env python
"""
Test AdminClient by requesting specific topic instead of listing all.

Sometimes list_topics() on AdminClient behaves differently than
getting metadata for a specific topic.
"""

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from tokenprovider import TokenProvider
import time

token_provider = TokenProvider()

config = {
    'bootstrap.servers': 'bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': token_provider.get_token,
    'socket.timeout.ms': 60000,
    'request.timeout.ms': 60000,
    'api.version.request.timeout.ms': 30000,
}

print("Creating AdminClient...")
admin = AdminClient(config)
print("✓ AdminClient created\n")

# Test 1: Try listing with specific topic filter
print("="*60)
print("Test 1: list_topics() for specific topic 'testtopic'")
print("="*60)
try:
    print("Calling list_topics(topic='testtopic', timeout=30)...")
    metadata = admin.list_topics(topic='testtopic', timeout=30)
    print(f"✓ Success!")
    if 'testtopic' in metadata.topics:
        topic_meta = metadata.topics['testtopic']
        print(f"  Topic 'testtopic': {len(topic_meta.partitions)} partitions")
    else:
        print(f"  Topic 'testtopic' not found")
except Exception as e:
    print(f"✗ Failed: {e}")

# Test 2: Try listing all topics
print("\n" + "="*60)
print("Test 2: list_topics() for all topics")
print("="*60)
try:
    print("Calling list_topics(timeout=30)...")
    metadata = admin.list_topics(timeout=30)
    print(f"✓ Success! Found {len(metadata.topics)} topics")
    for topic_name in list(metadata.topics.keys())[:5]:  # Show first 5
        if not topic_name.startswith('_'):
            print(f"  - {topic_name}")
except Exception as e:
    print(f"✗ Failed: {e}")

print()
