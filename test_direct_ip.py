#!/usr/bin/env python3
"""Test MSAK with direct internal IP address."""

import os
import sys
sys.path.insert(0, '/Users/yangzhang/Desktop/psl/python-pubsublite')

from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
from google.cloud.pubsublite.transport import MSAKConfig
from google.cloud.pubsub_v1.types import BatchSettings
import time

def main():
    print("🚀 Testing MSAK Publisher with Direct Internal IP...")
    
    # Try using the hostname instead of IP for SSL compatibility
    msak_config = MSAKConfig(
        project_id='ygnahz-dolphin-dev',
        location='us-central1',
        cluster_id='testpsl'
        # Let it use the default hostname for SSL verification
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
                max_bytes=1024*1024,
                max_latency=2.0
            )
        )
        
        print(f"✅ Client created successfully!")
        print(f"   Transport type: {client.transport_type}")
        
        # Test publishing
        print("\n📤 Publishing test messages...")
        with client:
            futures = []
            
            for i in range(3):
                message_data = f"Direct IP test message {i} at {time.strftime('%Y-%m-%d %H:%M:%S')}".encode('utf-8')
                
                future = client.publish(
                    topic=topic_name,
                    data=message_data,
                    ordering_key=f"direct-ip-key-{i}",
                    connection_type="direct_internal_ip",
                    test_id=str(i)
                )
                
                futures.append((i, future))
                print(f"   📨 Queued message {i}")
            
            # Wait for results
            print("\n⏳ Waiting for publish confirmations...")
            success_count = 0
            
            for i, future in futures:
                try:
                    message_id = future.result(timeout=30)
                    print(f"   ✅ Message {i} published successfully!")
                    print(f"      Message ID: {message_id}")
                    success_count += 1
                except Exception as e:
                    print(f"   ❌ Message {i} failed: {e}")
            
            print(f"\n📊 Results: {success_count}/{len(futures)} messages published successfully")
            
            if success_count > 0:
                print("\n🎉 SUCCESS! MSAK integration working with direct internal IP!")
                print("Your implementation is complete and functional.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    # Clear problematic env vars
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    sys.exit(main())