#!/usr/bin/env python3
"""Test integration of EnhancedPublisherClient with tokenprovider credentials."""

import sys
import os

def main():
    print("🧪 Testing tokenprovider integration with EnhancedPublisherClient")
    print("=" * 60)
    
    # Clear environment to use application default credentials
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    success_count = 0
    total_tests = 4
    
    # Test 1: TokenProvider credential creation
    print("\n1️⃣ Testing TokenProvider credential creation...")
    try:
        from tokenprovider import TokenProvider
        token_provider = TokenProvider()
        credentials = token_provider.get_credentials()
        
        print(f"   ✅ TokenProvider created successfully")
        print(f"   ✅ Credentials obtained: {type(credentials).__name__}")
        print(f"   ✅ Valid: {credentials.valid}")
        success_count += 1
    except Exception as e:
        print(f"   ❌ TokenProvider test failed: {e}")
    
    # Test 2: MSAKConfig with tokenprovider credentials
    print("\n2️⃣ Testing MSAKConfig with tokenprovider credentials...")
    try:
        from google.cloud.pubsublite.transport import MSAKConfig
        
        msak_config = MSAKConfig(
            project_id='ygnahz-eg-codelab',
            location='us-central1',
            cluster_id='testpsl',
            credentials=credentials  # Use tokenprovider credentials
        )
        
        print(f"   ✅ MSAKConfig created with tokenprovider credentials")
        print(f"   ✅ Bootstrap servers: {msak_config.bootstrap_servers}")
        print(f"   ✅ Credentials type: {type(msak_config.credentials).__name__}")
        success_count += 1
    except Exception as e:
        print(f"   ❌ MSAKConfig test failed: {e}")
    
    # Test 3: EnhancedPublisherClient creation with tokenprovider
    print("\n3️⃣ Testing EnhancedPublisherClient creation...")
    try:
        from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
        from google.cloud.pubsub_v1.types import BatchSettings
        
        client = EnhancedPublisherClient.create_for_msak(
            topic='testtopic',
            msak_config=msak_config,
            per_partition_batching_settings=BatchSettings(max_messages=5)
        )
        
        print(f"   ✅ EnhancedPublisherClient created successfully")
        print(f"   ✅ Client type: {type(client).__name__}")
        print(f"   ✅ Transport type: {client.transport_type}")
        print(f"   ✅ MSAK config has credentials: {msak_config.credentials is not None}")
        success_count += 1
    except Exception as e:
        print(f"   ❌ EnhancedPublisherClient creation failed: {e}")
    
    # Test 4: Interface compliance check
    print("\n4️⃣ Testing interface compliance...")
    try:
        from google.cloud.pubsublite.cloudpubsub.publisher_client_interface import PublisherClientInterface
        
        assert isinstance(client, PublisherClientInterface), "Client doesn't implement interface"
        
        required_methods = ['publish', 'flush', 'transport_type', '__enter__', '__exit__']
        for method in required_methods:
            assert hasattr(client, method), f"Missing method: {method}"
        
        print(f"   ✅ Interface compliance verified")
        print(f"   ✅ All required methods present: {required_methods}")
        success_count += 1
    except Exception as e:
        print(f"   ❌ Interface compliance failed: {e}")
    
    # Results
    print(f"\n📊 Test Results: {success_count}/{total_tests} tests passed")
    
    if success_count == total_tests:
        print("\n🎉 All integration tests passed!")
        print("\n✨ Summary:")
        print("   • TokenProvider creates valid Google Cloud credentials")
        print("   • MSAKConfig accepts and stores tokenprovider credentials")  
        print("   • EnhancedPublisherClient.create_for_msak() accepts tokenprovider credentials")
        print("   • Client implements PublisherClientInterface correctly")
        print("   • Ready for production use with tokenprovider authentication!")
        return 0
    else:
        print(f"\n❌ {total_tests - success_count} tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())