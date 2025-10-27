#!/usr/bin/env python
"""
Test AdminClient with tuned configuration for admin operations.

AdminClient has different requirements than Producer/Consumer.
"""

from confluent_kafka.admin import AdminClient
from tokenprovider import TokenProvider
import time

token_provider = TokenProvider()

# AdminClient-specific configuration
# Key differences from Producer/Consumer:
# 1. Needs higher connection timeouts
# 2. Needs explicit metadata refresh settings
# 3. May need different socket settings
config = {
    'bootstrap.servers': 'bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': token_provider.get_token,

    # AdminClient-specific tuning
    'socket.timeout.ms': 60000,  # 60s socket operations
    'socket.connection.setup.timeout.ms': 30000,  # 30s for initial connection
    'connections.max.idle.ms': 540000,  # Keep connections alive
    'metadata.max.age.ms': 30000,  # Force metadata refresh
    'api.version.request.timeout.ms': 30000,  # API version negotiation
    'request.timeout.ms': 60000,  # 60s for requests

    # Retry settings
    'reconnect.backoff.ms': 50,
    'reconnect.backoff.max.ms': 1000,
}

print("Creating AdminClient with tuned configuration...")
print(f"Bootstrap servers: {config['bootstrap.servers']}")
print(f"\nKey timeout settings:")
print(f"  socket.timeout.ms: {config['socket.timeout.ms']}")
print(f"  request.timeout.ms: {config['request.timeout.ms']}")
print(f"  socket.connection.setup.timeout.ms: {config['socket.connection.setup.timeout.ms']}")

admin = AdminClient(config)
print("\n✓ AdminClient created")

print("\n" + "="*60)
print("Attempting list_topics() with 60s timeout...")
print("This may take a while as AdminClient establishes connection...")
print("="*60 + "\n")

start_time = time.time()

try:
    # AdminClient may need time to establish initial connection
    print("Calling list_topics()...")
    metadata = admin.list_topics(timeout=60)

    elapsed = time.time() - start_time
    print(f"\n✓ Success! (took {elapsed:.2f}s)")
    print(f"  Cluster ID: {metadata.cluster_id}")
    print(f"  Total topics: {len(metadata.topics)}")

    print("\nTopics:")
    for topic_name, topic_metadata in metadata.topics.items():
        if not topic_name.startswith('_'):
            print(f"  - {topic_name}: {len(topic_metadata.partitions)} partitions")
            if topic_metadata.error:
                print(f"    Error: {topic_metadata.error}")

except Exception as e:
    elapsed = time.time() - start_time
    print(f"\n✗ Failed after {elapsed:.2f}s: {e}")
    print(f"   Exception type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
