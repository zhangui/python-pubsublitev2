#!/usr/bin/env python3
"""Simple MSAK test following Google's quickstart guide."""

import os
import sys
import time
from confluent_kafka import Producer
from google.auth import default
from google.auth.transport.requests import Request

from tokenprovider import TokenProvider

def test_msak():
    """Test MSAK connection and publishing."""
    
    print("🚀 Testing Google Managed Service for Apache Kafka...")
    
    # MSAK cluster configuration
    PROJECT_ID = 'ygnahz-eg-codelab'
    REGION = 'us-central1'
    CLUSTER_ID = 'testpsl'
    TOPIC = 'testtopic'
    
    # Bootstrap server format from documentation
    bootstrap_servers = f'bootstrap.{CLUSTER_ID}.{REGION}.managedkafka.{PROJECT_ID}.cloud.goog:9092'
    
    print(f"📡 Bootstrap servers: {bootstrap_servers}")
    print(f"📝 Topic: {TOPIC}")
    token_provider = TokenProvider()
    
    # Confluent Kafka configuration
    config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
        # 'client.id': 'python-msak-test',
        # SSL configuration
        # 'ssl.endpoint.identification.algorithm': 'none',
        # 'ssl.ca.location': 'probe',  # Use system CA certificates
        # Additional settings
        # 'socket.timeout.ms': 30000,
        # 'api.version.request.timeout.ms': 30000,
    }
    
    try:
        # Create producer
        print("\n🔧 Creating Kafka producer...")
        producer = Producer(config)
        print("✅ Producer created successfully!")
        
        # Test connection
        print("\n🔌 Testing connection...")
        metadata = producer.list_topics(timeout=10)
        print(f"✅ Connected! Found {len(metadata.topics)} topics")
        
        # Publish test message
        print(f"\n📤 Publishing message to {TOPIC}...")
        
        def delivery_callback(err, msg):
            if err:
                print(f"❌ Delivery failed: {err}")
            else:
                print(f"✅ Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
        
        message = f"Test message from Python at {time.strftime('%Y-%m-%d %H:%M:%S')}"
        producer.produce(
            TOPIC,
            key='test-key',
            value=message.encode('utf-8'),
            callback=delivery_callback
        )
        
        # Wait for delivery
        print("⏳ Waiting for delivery...")
        producer.flush(timeout=10)
        
        print("\n🎉 Test completed successfully!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n🔍 Troubleshooting:")
        print("1. Ensure you're running from a VM in the same VPC as the MSAK cluster")
        print("2. Check authentication: gcloud auth application-default login")
        print("3. Verify IAM permissions (roles/managedkafka.client)")
        print("4. Check if topic exists: gcloud managed-kafka topics list --cluster=testpsl --location=us-central1")
        return 1

if __name__ == "__main__":
    # Clear any conflicting environment variables
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    sys.exit(test_msak())