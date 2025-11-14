#!/usr/bin/env python
"""Quick test to verify cursor service imports work correctly."""

import sys
print(f"Python path: {sys.path[:3]}")

print("\n1. Testing basic imports...")
try:
    from google.cloud.pubsublite_v1.services.cursor_service import CursorServiceClient
    print(f"✓ CursorServiceClient imported: {CursorServiceClient}")
except Exception as e:
    print(f"✗ Failed to import CursorServiceClient: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n2. Checking transport registry...")
try:
    registry = CursorServiceClient._transport_registry
    print(f"✓ Transport registry keys: {list(registry.keys())}")

    if "kafka" in registry:
        print(f"✓ Kafka transport IS registered")
        print(f"  Kafka transport class: {registry['kafka']}")
    else:
        print(f"✗ Kafka transport NOT registered")
        print(f"  Available: {list(registry.keys())}")
except Exception as e:
    print(f"✗ Error checking registry: {e}")
    import traceback
    traceback.print_exc()

print("\n3. Testing get_transport_class...")
try:
    transport_class = CursorServiceClient.get_transport_class("kafka")
    print(f"✓ get_transport_class('kafka') returned: {transport_class}")
except KeyError as e:
    print(f"✗ KeyError getting kafka transport: {e}")
except Exception as e:
    print(f"✗ Error getting kafka transport: {e}")
    import traceback
    traceback.print_exc()

print("\n4. Testing Kafka transport import directly...")
try:
    from google.cloud.pubsublite_v1.services.cursor_service.transports.kafka import CursorServiceKafkaTransport
    print(f"✓ Direct import successful: {CursorServiceKafkaTransport}")
except ImportError as e:
    print(f"✗ ImportError: {e}")
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n5. Testing client creation with kafka transport...")
try:
    # Minimal config just to test creation
    test_config = {
        'bootstrap.servers': 'test:9092',
        'group.id': 'test-group',
    }
    client = CursorServiceClient(
        transport="kafka",
        consumer_config=test_config
    )
    print(f"✓ Client created: {client}")
    print(f"✓ Transport type: {type(client._transport).__name__}")
except Exception as e:
    print(f"✗ Failed to create client: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("Import test complete!")
print("=" * 60)
