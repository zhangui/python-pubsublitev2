#!/usr/bin/env python3
"""MSAK test with corrected OAuth implementation matching GcpLoginCallbackHandler."""

import os
import sys
import time
import json
from confluent_kafka import Producer
from google.auth import default
from google.auth.transport.requests import Request

def gcp_login_callback_handler(config_str):
    """
    OAuth callback that exactly mimics GcpLoginCallbackHandler.
    
    Key differences from our previous implementation:
    1. Must handle token refresh correctly
    2. Must return proper OAuth Bearer token format
    3. Must handle scopes exactly like Java version
    """
    try:
        print(f"🔐 OAuth callback called with config: {config_str}")
        
        # Get credentials exactly like GcpLoginCallbackHandler
        credentials, project = default()
        print(f"   📋 Project: {project}")
        print(f"   🔑 Credential type: {type(credentials).__name__}")
        
        # Apply the exact same scope as GcpLoginCallbackHandler
        scopes = ['https://www.googleapis.com/auth/cloud-platform']
        if hasattr(credentials, 'with_scopes'):
            credentials = credentials.with_scopes(scopes)
            print(f"   ✅ Applied scopes: {scopes}")
        else:
            print(f"   ⚠️  Credentials don't support scopes: {type(credentials)}")
        
        # Force refresh to ensure valid token
        request = Request()
        try:
            credentials.refresh(request)
            print(f"   🔄 Token refreshed successfully")
        except Exception as refresh_error:
            print(f"   ❌ Token refresh failed: {refresh_error}")
            # Try to continue anyway
        
        # Verify token is valid
        if not credentials.valid:
            print(f"   ❌ Credentials are not valid after refresh")
            return "", 0
        
        # Get token details
        token = credentials.token
        if not token:
            print(f"   ❌ No access token available")
            return "", 0
            
        print(f"   ✅ Access token obtained")
        print(f"   📏 Token length: {len(token)}")
        print(f"   🔤 Token prefix: {token[:50]}...")
        
        # Calculate expiry timestamp
        if hasattr(credentials, 'expiry') and credentials.expiry:
            expiry_timestamp = credentials.expiry.timestamp()
            expiry_str = credentials.expiry.strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"   ⏰ Token expires: {expiry_str}")
        else:
            # Default to 1 hour if no expiry
            expiry_timestamp = time.time() + 3600
            print(f"   ⏰ No expiry info, defaulting to +1 hour")
        
        # Return in the format expected by confluent-kafka
        return token, expiry_timestamp
        
    except Exception as e:
        print(f"   💥 OAuth callback exception: {e}")
        import traceback
        traceback.print_exc()
        return "", 0

def test_msak_with_fixed_auth():
    """Test MSAK with corrected authentication."""
    
    print("🚀 Testing MSAK with corrected OAuth authentication...")
    
    # Configuration
    PROJECT_ID = 'ygnahz-eg-codelab'
    REGION = 'us-central1'
    CLUSTER_ID = 'testpsl'
    TOPIC = 'testtopic'
    
    bootstrap_servers = f'bootstrap.{CLUSTER_ID}.{REGION}.managedkafka.{PROJECT_ID}.cloud.goog:9092'
    print(f"📡 Bootstrap servers: {bootstrap_servers}")
    
    # Producer configuration matching working Java setup
    config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'OAUTHBEARER',
        'oauth_cb': gcp_login_callback_handler,
        
        # Match Java client behavior more closely
        'client.id': 'python-msak-fixed-auth',
        'acks': 'all',
        'retries': 3,
        'retry.backoff.ms': 100,
        
        # SSL configuration
        'ssl.endpoint.identification.algorithm': 'https',
        'ssl.ca.location': 'probe',
        
        # Connection timeouts
        'socket.timeout.ms': 60000,
        'request.timeout.ms': 30000,
        'api.version.request.timeout.ms': 10000,
        
        # SASL settings - try to match Java more closely
        'sasl.oauthbearer.config': 'principalClaimName=sub',
        
        # Enable debug for SASL/security
        'debug': 'security,broker,protocol',
        'log_level': 0,
    }
    
    try:
        print("\n🔧 Creating Kafka producer with fixed authentication...")
        producer = Producer(config)
        print("✅ Producer created successfully!")
        
        print("\n🔌 Testing metadata request (this will trigger OAuth)...")
        metadata = producer.list_topics(timeout=15)
        
        print(f"✅ Metadata received successfully!")
        print(f"   🏢 Brokers: {len(metadata.brokers)}")
        print(f"   📝 Topics: {len(metadata.topics)}")
        
        if TOPIC in metadata.topics:
            topic_metadata = metadata.topics[TOPIC]
            print(f"   ✅ Topic '{TOPIC}' found with {len(topic_metadata.partitions)} partitions")
        else:
            print(f"   ⚠️  Topic '{TOPIC}' not found")
            print(f"   📋 Available topics: {list(metadata.topics.keys())[:10]}")
        
        print(f"\n📤 Testing message production...")
        
        message_sent = False
        def delivery_callback(err, msg):
            nonlocal message_sent
            if err:
                print(f"   ❌ Message delivery failed: {err}")
            else:
                print(f"   ✅ Message delivered successfully!")
                print(f"      Topic: {msg.topic()}")
                print(f"      Partition: {msg.partition()}")
                print(f"      Offset: {msg.offset()}")
                message_sent = True
        
        test_message = f"Fixed auth test from Python - {time.strftime('%Y-%m-%d %H:%M:%S')}"
        producer.produce(
            TOPIC,
            key='fixed-auth-test',
            value=test_message.encode('utf-8'),
            callback=delivery_callback
        )
        
        print("⏳ Waiting for message delivery...")
        producer.flush(timeout=15)
        
        if message_sent:
            print("\n🎉 SUCCESS! MSAK authentication and message production working!")
        else:
            print("\n❌ Message was not delivered successfully")
        
        return 0 if message_sent else 1
        
    except Exception as e:
        print(f"\n💥 Error: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    # Ensure clean authentication environment
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    print("🔍 Environment check:")
    print(f"   GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'Not set')}")
    
    sys.exit(test_msak_with_fixed_auth())