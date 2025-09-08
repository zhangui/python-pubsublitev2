#!/usr/bin/env python3
"""Test different OAuth callback return formats."""

import os
import sys
sys.path.insert(0, '/Users/yangzhang/Desktop/psl/python-pubsublite')

from confluent_kafka import Producer
from google.auth import default
from google.auth.transport.requests import Request
import time

def oauth_cb_basic(config):
    """Basic OAuth callback - returns (token, expiry)"""
    try:
        credentials, _ = default()
        if hasattr(credentials, 'with_scopes'):
            credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])
        
        request = Request()
        credentials.refresh(request)
        
        expiry = credentials.expiry.timestamp() if credentials.expiry else time.time() + 3600
        print(f"Basic format: returning (token, {expiry})")
        return credentials.token, expiry
    except Exception as e:
        print(f"OAuth error: {e}")
        return "", 0

def oauth_cb_extended(config):
    """Extended OAuth callback - returns (token, expiry, principal, extensions)"""
    try:
        credentials, _ = default()
        if hasattr(credentials, 'with_scopes'):
            credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])
        
        request = Request()
        credentials.refresh(request)
        
        expiry = credentials.expiry.timestamp() if credentials.expiry else time.time() + 3600
        
        # Try extended format with principal and extensions
        principal = None  # Can be None or user identifier
        extensions = {}   # Additional SASL extensions
        
        print(f"Extended format: returning (token, {expiry}, {principal}, {extensions})")
        return credentials.token, expiry, principal, extensions
    except Exception as e:
        print(f"OAuth error: {e}")
        return "", 0

def test_oauth_formats():
    """Test different OAuth return formats."""
    
    base_config = {
        'bootstrap.servers': 'bootstrap.testpsl.us-central1.managedkafka.ygnahz-dolphin-dev.cloud.goog:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'OAUTHBEARER',
        'client.id': 'oauth-format-test',
        'ssl.endpoint.identification.algorithm': 'none',
        'ssl.ca.location': 'probe',
    }
    
    formats = [
        ("Basic Format (token, expiry)", oauth_cb_basic),
        ("Extended Format (token, expiry, principal, extensions)", oauth_cb_extended),
    ]
    
    for name, oauth_cb in formats:
        print(f"\n=== Testing {name} ===")
        
        config = base_config.copy()
        config['oauth_cb'] = oauth_cb
        
        try:
            producer = Producer(config)
            print("✅ Producer created")
            
            # Try to trigger OAuth
            producer.poll(1.0)
            print("✅ OAuth callback format accepted")
            
        except Exception as e:
            print(f"❌ Failed: {e}")

if __name__ == "__main__":
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    test_oauth_formats()