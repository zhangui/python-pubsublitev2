#!/usr/bin/env python3
"""Test script for MSAK integration with real Google Cloud cluster."""

from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
from google.cloud.pubsublite.transport import MSAKConfig
from google.cloud.pubsub_v1.types import BatchSettings
import time
import sys

def main():
    print("🚀 Testing MSAK Publisher with real Google Cloud cluster...")
    
    # Get credentials using tokenprovider style
    try:
        from tokenprovider import TokenProvider
        token_provider = TokenProvider()
        credentials = token_provider.get_credentials()
        print(f"🔐 Using tokenprovider credentials for authentication")
    except Exception as e:
        print(f"⚠️  Failed to get tokenprovider credentials: {e}")
        print("   Falling back to default credentials")
        credentials = None
    
    # Configure your cluster with credentials
    msak_config = MSAKConfig(
        project_id='ygnahz-eg-codelab',
        location='us-central1',
        cluster_id='testpsl',
        credentials=credentials
    )
    
    topic_name = 'testtopic'
    
    print(f"📡 Bootstrap servers: {msak_config.bootstrap_servers}")
    print(f"📝 Topic: {topic_name}")
    
    try:
        # Create client
        print("🔧 Creating MSAK publisher client...")
        client = EnhancedPublisherClient.create_for_msak(
            topic=topic_name,
            msak_config=msak_config
            # per_partition_batching_settings=BatchSettings(
            #     max_messages=10,
            #     max_bytes=1024*1024,  # 1MB
            # )
        )
        
        print(f"✅ Client created successfully!")
        print(f"   Transport type: {client.transport_type}")
        
        # Test publishing
        print("\n📤 Publishing test messages...")
        with client:
            futures = []
            
            for i in range(5):
                message_data = f"Test message {i} from Python MSAK client at {time.strftime('%Y-%m-%d %H:%M:%S')}".encode('utf-8')
                
                future = client.publish(
                    topic=topic_name,
                    data=message_data,
                    ordering_key=f"test-key-{i % 2}",  # Alternates between two keys for partitioning
                    message_type="test",
                    sender="python-msak-client",
                    timestamp=str(int(time.time()))
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
            
            # Flush any remaining messages
            print("🔄 Flushing remaining messages...")
            client.flush(timeout=10)
            
    except ImportError as e:
        print(f"❌ Missing dependencies: {e}")
        print("💡 Install with: pip install google-cloud-pubsublite[msak]")
        return 1
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔍 Troubleshooting tips:")
        print("   1. Ensure you're authenticated: gcloud auth application-default login")
        print("   2. Verify your cluster exists: gcloud managed-kafka clusters list")
        print("   3. Check topic exists: gcloud managed-kafka topics list --cluster=testpsl --location=us-central1")
        print("   4. Ensure you have Kafka producer permissions")
        print("\n⚠️  IMPORTANT: MSAK clusters are only accessible from within the VPC network.")
        print("   If you're running this locally, you need to:")
        print("   a) Run this from a Compute Engine VM in the same VPC as your MSAK cluster")
        print("   b) Set up Cloud VPN or Cloud Interconnect to access the cluster")
        print("   c) Use a bastion host or proxy to connect to the cluster")
        return 1
    
    print("\n🎉 Test completed successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main())