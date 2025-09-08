#!/usr/bin/env python3
"""MSAK test using official Google Python approach with TokenProvider."""

import os
import sys
import time
from confluent_kafka import Producer
from tokenprovider import TokenProvider

def test_msak_official():
    """Test MSAK using the official Google Python approach."""
    
    print("🚀 Testing MSAK with official Google Python TokenProvider...")
    
    # Configuration
    PROJECT_ID = 'ygnahz-eg-codelab'
    REGION = 'us-central1'
    CLUSTER_ID = 'testpsl'
    TOPIC = 'testtopic'
    
    bootstrap_servers = f'bootstrap.{CLUSTER_ID}.{REGION}.managedkafka.{PROJECT_ID}.cloud.goog:9092'
    print(f"📡 Bootstrap servers: {bootstrap_servers}")
    
    # Create token provider (Google's approach)
    token_provider = TokenProvider()
    
    # Producer configuration exactly like Google's official example
    config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',  # Note: mechanisms (plural) like in Google example
        'oauth_cb': token_provider.get_token,
        
        # Additional settings for better debugging
        'client.id': 'python-msak-official',
        'debug': 'security,broker,protocol',
        'log_level': 0,
    }
    
    try:
        print("\n🔧 Creating Kafka producer with official Google approach...")
        producer = Producer(config)
        print("✅ Producer created successfully!")
        
        print("\n🔌 Testing connection and authentication...")
        metadata = producer.list_topics(timeout=15)
        
        print(f"✅ Connected and authenticated successfully!")
        print(f"   🏢 Brokers: {len(metadata.brokers)}")
        print(f"   📝 Topics: {len(metadata.topics)}")
        
        # Check if our topic exists
        if TOPIC in metadata.topics:
            topic_info = metadata.topics[TOPIC]
            print(f"   ✅ Topic '{TOPIC}' found with {len(topic_info.partitions)} partitions")
        else:
            print(f"   ⚠️  Topic '{TOPIC}' not found")
            available_topics = list(metadata.topics.keys())[:5]
            print(f"   📋 First 5 topics: {available_topics}")
        
        print(f"\n📤 Testing message production to {TOPIC}...")
        
        message_delivered = False
        def delivery_callback(err, msg):
            nonlocal message_delivered
            if err:
                print(f"   ❌ Message delivery failed: {err}")
            else:
                print(f"   ✅ Message delivered successfully!")
                print(f"      Topic: {msg.topic()}")
                print(f"      Partition: {msg.partition()}")
                print(f"      Offset: {msg.offset()}")
                print(f"      Timestamp: {msg.timestamp()}")
                message_delivered = True
        
        # Send test message
        test_message = f"Official Google approach test - {time.strftime('%Y-%m-%d %H:%M:%S')}"
        producer.produce(
            TOPIC,
            key='official-test',
            value=test_message.encode('utf-8'),
            callback=delivery_callback
        )
        
        print("⏳ Waiting for message delivery...")
        producer.flush(timeout=15)
        
        if message_delivered:
            print("\n🎉 SUCCESS! Official Google approach works!")
        else:
            print("\n❌ Message was not delivered")
            
        return 0 if message_delivered else 1
        
    except Exception as e:
        print(f"\n💥 Error: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    # Clean environment
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    print("🔍 Environment check:")
    print(f"   GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'Not set')}")
    
    sys.exit(test_msak_official())