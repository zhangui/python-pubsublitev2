#!/usr/bin/env python3
"""Debug SSL connection to MSAK."""

import os
import sys
sys.path.insert(0, '/Users/yangzhang/Desktop/psl/python-pubsublite')

from confluent_kafka import Producer
from google.auth import default
from google.auth.transport.requests import Request
import time

def oauth_cb(config):
    try:
        credentials, _ = default()
        
        # Apply scopes if the credential type supports it
        if hasattr(credentials, 'with_scopes'):
            credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])
        
        request = Request()
        credentials.refresh(request)
        
        expiry = credentials.expiry.timestamp() if credentials.expiry else time.time() + 3600
        return credentials.token, expiry
    except Exception as e:
        print(f"OAuth error: {e}")
        return None, 0

def test_ssl_configs():
    """Test different SSL configurations."""
    
    configs = [
        # Config 1: Hostname with strict SSL
        {
            'name': 'Hostname with strict SSL',
            'bootstrap.servers': 'bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'OAUTHBEARER',
            'oauth_cb': oauth_cb,
        },
        # Config 2: Hostname with relaxed SSL
        {
            'name': 'Hostname with relaxed SSL',
            'bootstrap.servers': 'bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'OAUTHBEARER',
            'oauth_cb': oauth_cb,
            'ssl.endpoint.identification.algorithm': 'none',
            'ssl.ca.location': 'probe',
        },
        # Config 3: IP with relaxed SSL  
        {
            'name': 'IP with relaxed SSL',
            'bootstrap.servers': '10.128.0.26:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'OAUTHBEARER',
            'oauth_cb': oauth_cb,
            'ssl.endpoint.identification.algorithm': 'none',
            'ssl.ca.location': 'probe',
        },
    ]
    
    for config in configs:
        print(f"\n=== Testing: {config['name']} ===")
        
        # Remove name from config before creating producer
        test_config = {k: v for k, v in config.items() if k != 'name'}
        
        try:
            producer = Producer(test_config)
            print("✅ Producer created successfully")
            
            # Test connection
            producer.poll(1.0)
            print("✅ Connection test passed")
            
            # Try to produce a test message
            producer.produce(
                'testtopic',
                key='test-key',
                value=f'SSL test from {config["name"]}',
                callback=lambda err, msg: print(f"Message result: {'Success' if not err else f'Error: {err}'}")
            )
            
            producer.flush(5.0)
            print("✅ Message production test completed")
            
        except Exception as e:
            print(f"❌ Failed: {e}")

if __name__ == "__main__":
    # Clear environment
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    test_ssl_configs()