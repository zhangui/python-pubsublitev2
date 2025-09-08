#!/usr/bin/env python3
"""Test script using the exact bootstrap server from gcloud command."""

import os
import sys
sys.path.insert(0, '/Users/yangzhang/Desktop/psl/python-pubsublite')

from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
from google.cloud.pubsublite.transport import MSAKConfig
from google.cloud.pubsub_v1.types import BatchSettings
import time

def main():
    print("🚀 Testing MSAK Publisher with direct bootstrap server...")
    
    # Use the exact bootstrap server from your gcloud command
    msak_config = MSAKConfig(
        project_id='ygnahz-eg-codelab',
        location='us-central1',
        cluster_id='testpsl',
        endpoint_override='bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092'
    )
    
    topic_name = 'testtopic'
    
    print(f"📡 Bootstrap servers: {msak_config.bootstrap_servers}")
    print(f"📝 Topic: {topic_name}")
    
    try:
        # Create client
        print("🔧 Creating MSAK publisher client...")
        client = EnhancedPublisherClient.create_for_msak(
            topic=topic_name,
            msak_config=msak_config,
            per_partition_batching_settings=BatchSettings(
                max_messages=3,
                max_bytes=1024,
                max_latency=1.0  # 1 second
            )
        )
        
        print(f"✅ Client created successfully!")
        print(f"   Transport type: {client.transport_type}")
        
        # Test publishing just one message
        print("\n📤 Publishing test message...")
        with client:
            message_data = f"Test message from Python MSAK client at {time.strftime('%Y-%m-%d %H:%M:%S')}".encode('utf-8')
            
            future = client.publish(
                topic=topic_name,
                data=message_data,
                ordering_key="test-key",
                test_attr="python-msak-test"
            )
            
            print("   📨 Message queued, waiting for confirmation...")
            
            try:
                message_id = future.result(timeout=10)
                print(f"   ✅ Message published successfully!")
                print(f"      Message ID: {message_id}")
                return 0
            except Exception as e:
                print(f"   ❌ Message failed: {e}")
                return 1
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    # Clear the problematic environment variable
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    sys.exit(main())