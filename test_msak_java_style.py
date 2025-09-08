#!/usr/bin/env python3
"""MSAK test using Java-style authentication configuration."""

import os
import sys
import time
from confluent_kafka import Producer
from google.auth import default
from google.auth.transport.requests import Request

def gcp_style_oauth_callback(config_str):
    """
    OAuth callback that mimics GcpLoginCallbackHandler behavior.
    
    GcpLoginCallbackHandler does:
    1. Gets Application Default Credentials
    2. Applies cloud-platform scope
    3. Returns properly formatted OAuth bearer token
    """
    try:
        print("🔐 GCP OAuth callback invoked...")
        
        # Get Application Default Credentials (same as GcpLoginCallbackHandler)
        credentials, project = default()
        print(f"   Project: {project}")
        
        # Apply cloud-platform scope (same as Java version)
        if hasattr(credentials, 'with_scopes'):
            credentials = credentials.with_scopes([
                'https://www.googleapis.com/auth/cloud-platform'
            ])
            print("   ✅ Applied cloud-platform scope")
        
        # Refresh token if needed
        if not credentials.valid:
            print("   🔄 Refreshing credentials...")
            request = Request()
            credentials.refresh(request)
        
        # Get token and expiry (format like Java version)
        token = credentials.token
        expiry = credentials.expiry.timestamp() if credentials.expiry else time.time() + 3600
        
        print(f"   ✅ Token obtained (expires: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expiry))})")
        print(f"   Token prefix: {token[:20]}...")
        
        return token, expiry
        
    except Exception as e:
        print(f"   ❌ OAuth callback error: {e}")
        import traceback
        traceback.print_exc()
        return "", 0

def test_msak_java_style():
    """Test MSAK with Java-style authentication."""
    
    print("🚀 Testing MSAK with Java-style authentication...")
    
    # Configuration matching Java approach
    PROJECT_ID = 'ygnahz-eg-codelab'
    REGION = 'us-central1'
    CLUSTER_ID = 'testpsl'
    TOPIC = 'testtopic'
    
    bootstrap_servers = f'bootstrap.{CLUSTER_ID}.{REGION}.managedkafka.{PROJECT_ID}.cloud.goog:9092'
    print(f"📡 Bootstrap servers: {bootstrap_servers}")
    
    # Python configuration that mimics Java client.properties
    config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'OAUTHBEARER',
        'oauth_cb': gcp_style_oauth_callback,
        
        # Additional settings to match Java behavior
        'client.id': 'python-msak-gcp-style',
        'api.version.request': 'true',
        'api.version.request.timeout.ms': 10000,
        'socket.timeout.ms': 30000,
        'request.timeout.ms': 30000,
        
        # SSL settings
        'ssl.endpoint.identification.algorithm': 'https',
        'ssl.ca.location': 'probe',
        
        # Debug settings
        'debug': 'security,broker,protocol',
        'log_level': 0,
    }
    
    try:
        print("\n🔧 Creating Kafka producer with GCP-style auth...")
        producer = Producer(config)
        print("✅ Producer created successfully!")
        
        print("\n🔌 Testing admin client functionality...")
        metadata = producer.list_topics(timeout=10)
        print(f"✅ Connected! Metadata received")
        print(f"   Broker count: {len(metadata.brokers)}")
        print(f"   Topic count: {len(metadata.topics)}")
        
        if TOPIC in metadata.topics:
            print(f"   ✅ Topic '{TOPIC}' found")
        else:
            print(f"   ❌ Topic '{TOPIC}' not found")
            print(f"   Available topics: {list(metadata.topics.keys())[:5]}")
        
        print(f"\n📤 Testing message production to {TOPIC}...")
        
        def delivery_callback(err, msg):
            if err:
                print(f"   ❌ Delivery failed: {err}")
            else:
                print(f"   ✅ Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
        
        message = f"Java-style auth test from Python at {time.strftime('%Y-%m-%d %H:%M:%S')}"
        producer.produce(
            TOPIC,
            key='java-style-test',
            value=message.encode('utf-8'),
            callback=delivery_callback
        )
        
        print("⏳ Waiting for delivery...")
        producer.flush(timeout=10)
        
        print("\n🎉 Java-style authentication test completed successfully!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    # Clear any conflicting environment variables
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    sys.exit(test_msak_java_style())