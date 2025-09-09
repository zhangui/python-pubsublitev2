#!/usr/bin/env python3
"""Test EnhancedPublisherClient MSAK support using tokenprovider credentials."""

from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
from google.cloud.pubsublite.transport import MSAKConfig
from google.cloud.pubsub_v1.types import BatchSettings
import time
import sys

def main():
    print("🚀 Testing EnhancedPublisherClient MSAK with tokenprovider credentials...")
    
    # Get credentials using tokenprovider style
    try:
        from tokenprovider import TokenProvider
        token_provider = TokenProvider()
        credentials = token_provider.get_credentials()
        print(f"✅ Obtained tokenprovider credentials")
        print(f"   Credential type: {type(credentials).__name__}")
        print(f"   Service account: {getattr(credentials, 'service_account_email', 'N/A')}")
    except Exception as e:
        print(f"❌ Failed to get tokenprovider credentials: {e}")
        return 1
    
    # Configure cluster with tokenprovider credentials
    msak_config = MSAKConfig(
        project_id='ygnahz-eg-codelab',
        location='us-central1',
        cluster_id='testpsl',
        credentials=credentials  # Pass tokenprovider credentials
    )
    
    topic_name = 'testtopic'
    
    print(f"📡 Bootstrap servers: {msak_config.bootstrap_servers}")
    print(f"📝 Topic: {topic_name}")
    
    try:
        # Create MSAK client with tokenprovider credentials
        print("\n🔧 Creating EnhancedPublisherClient for MSAK...")
        client = EnhancedPublisherClient.create_for_msak(
            topic=topic_name,
            msak_config=msak_config,
            per_partition_batching_settings=BatchSettings(
                max_messages=10,
                max_bytes=1024*1024,  # 1MB
            )
        )
        
        print(f"✅ Client created successfully!")
        print(f"   Client type: {type(client).__name__}")
        print(f"   Transport type: {client.transport_type}")
        print(f"   Uses tokenprovider credentials: {msak_config.credentials is not None}")
        
        # Test publishing with tokenprovider credentials
        print("\n📤 Publishing test messages with tokenprovider auth...")
        with client:
            futures = []
            
            for i in range(3):
                message_data = f"Enhanced client test message {i} with tokenprovider - {time.strftime('%Y-%m-%d %H:%M:%S')}".encode('utf-8')
                
                future = client.publish(
                    topic=topic_name,
                    data=message_data,
                    ordering_key=f"enhanced-key-{i % 2}",
                    message_type="enhanced_test",
                    sender="enhanced-publisher-tokenprovider",
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
            
            print(f"\n📊 Results: {success_count}/{len(futures)} messages published")
            
            # Flush remaining messages
            print("🔄 Flushing remaining messages...")
            client.flush(timeout=10)
            
            if success_count > 0:
                print("\n🎉 SUCCESS! EnhancedPublisherClient with tokenprovider credentials works!")
                return 0
            else:
                print("\n❌ No messages were successfully published")
                return 1
            
    except ImportError as e:
        print(f"❌ Missing dependencies: {e}")
        print("💡 Ensure confluent-kafka is installed")
        return 1
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"   Exception type: {type(e).__name__}")
        import traceback
        print(f"   Full traceback:\n{traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main())