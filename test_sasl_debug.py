#!/usr/bin/env python3
"""Debug SASL OAUTHBEARER authentication for MSAK."""

import os
import sys
sys.path.insert(0, '/Users/yangzhang/Desktop/psl/python-pubsublite')

from confluent_kafka import Producer
from google.auth import default
from google.auth.transport.requests import Request
import time

def oauth_cb(config):
    print(f"OAuth callback called with config: {config}")
    try:
        credentials, project = default()
        print(f"Got credentials for project: {project}")
        
        # Apply scopes if the credential type supports it
        if hasattr(credentials, 'with_scopes'):
            credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])
            print("Applied cloud-platform scope")
        
        request = Request()
        credentials.refresh(request)
        print(f"Token refreshed successfully, length: {len(credentials.token)}")
        
        # Debug token details
        print(f"Token starts with: {credentials.token[:50]}...")
        print(f"Token type: {type(credentials.token)}")
        print(f"Credentials type: {type(credentials)}")
        
        expiry = credentials.expiry.timestamp() if credentials.expiry else time.time() + 3600
        print(f"Token expires at: {expiry}")
        
        return credentials.token, expiry
    except Exception as e:
        print(f"OAuth callback failed: {e}")
        import traceback
        traceback.print_exc()
        return None, 0

def test_sasl_connection():
    """Test actual SASL OAUTHBEARER connection."""
    
    print("=== SASL OAUTHBEARER Debug Test ===\n")
    
    # Test with hostname (most likely to work)
    config = {
        'bootstrap.servers': 'bootstrap.testpsl.us-central1.managedkafka.ygnahz-dolphin-dev.cloud.goog:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'OAUTHBEARER',
        'oauth_cb': oauth_cb,
        'client.id': 'sasl-debug-client',
        'socket.timeout.ms': 30000,
        'api.version.request.timeout.ms': 30000,
        'ssl.endpoint.identification.algorithm': 'none',
        'ssl.ca.location': 'probe',
        'debug': 'security,broker,protocol',  # Enable SASL debugging
    }
    
    print("Configuration:")
    for k, v in config.items():
        if k != 'oauth_cb':
            print(f"  {k}: {v}")
    
    try:
        print("\n1. Creating producer...")
        producer = Producer(config)
        print("✅ Producer created successfully")
        
        print("\n2. Testing initial connection (this will trigger SASL)...")
        # This will trigger the OAuth callback and SASL handshake
        producer.poll(5.0)  # Wait longer for connection
        print("✅ Initial poll completed")
        
        print("\n3. Testing metadata request...")
        # Try to get cluster metadata - this requires successful authentication
        metadata = producer.list_topics(timeout=10)
        print(f"✅ Got cluster metadata: {len(metadata.topics)} topics")
        
        print("\n4. Testing message production...")
        def delivery_callback(err, msg):
            if err:
                print(f"❌ Message delivery failed: {err}")
            else:
                print(f"✅ Message delivered: {msg.topic()}[{msg.partition()}] @ {msg.offset()}")
        
        producer.produce(
            'testtopic',
            key='sasl-test-key',
            value=f'SASL test message at {time.strftime("%Y-%m-%d %H:%M:%S")}',
            callback=delivery_callback
        )
        
        print("Message queued, waiting for delivery...")
        producer.flush(10.0)
        print("✅ Message flush completed")
        
    except Exception as e:
        print(f"❌ SASL connection failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Clear environment
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    test_sasl_connection()