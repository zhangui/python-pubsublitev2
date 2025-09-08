#!/usr/bin/env python3
"""Test MSAK with direct IP address instead of hostname."""

import os
import sys
import time
sys.path.insert(0, '/Users/yangzhang/Desktop/psl/python-pubsublite')

def test_dns_resolution():
    """Test DNS resolution for MSAK hostname."""
    import subprocess
    
    print("=== DNS Resolution Tests ===")
    hostname = "bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog"
    
    # Test nslookup
    print(f"1. Testing nslookup for {hostname}")
    try:
        result = subprocess.run(['nslookup', hostname], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✅ nslookup succeeded:")
            print(result.stdout)
        else:
            print("❌ nslookup failed:")
            print(result.stderr)
    except Exception as e:
        print(f"❌ nslookup error: {e}")
    
    # Test with Google DNS
    print(f"\n2. Testing with Google DNS (8.8.8.8)")
    try:
        result = subprocess.run(['nslookup', hostname, '8.8.8.8'], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✅ Google DNS lookup succeeded:")
            print(result.stdout)
        else:
            print("❌ Google DNS lookup failed:")
            print(result.stderr)
    except Exception as e:
        print(f"❌ Google DNS lookup error: {e}")
    
    # Check DNS servers
    print(f"\n3. Current DNS configuration:")
    try:
        with open('/etc/resolv.conf', 'r') as f:
            print(f.read())
    except Exception as e:
        print(f"❌ Could not read /etc/resolv.conf: {e}")

def test_msak_direct_ip():
    """Test MSAK with direct IP address."""
    print("\n=== MSAK Direct IP Test ===")
    
    try:
        from google.cloud.pubsublite.transport import MSAKConfig
        from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
        from google.cloud.pubsub_v1.types import BatchSettings
        
        # Use direct IP instead of hostname
        msak_config = MSAKConfig(
            project_id='ygnahz-eg-codelab',
            location='us-central1',
            cluster_id='testpsl',
            endpoint_override='10.128.0.26:9092'  # Direct bootstrap IP
        )
        
        print(f"Testing with direct IP: {msak_config.bootstrap_servers}")
        
        # Create client
        client = EnhancedPublisherClient.create_for_msak(
            topic='testtopic',
            msak_config=msak_config,
            per_partition_batching_settings=BatchSettings(
                max_messages=3,
                max_bytes=1024*1024,
                max_latency=2.0
            )
        )
        
        print("✅ Client created successfully!")
        
        # Test publishing
        with client:
            print("📤 Publishing test message...")
            
            future = client.publish(
                topic='testtopic',
                data=f'Test message with direct IP at {time.strftime("%Y-%m-%d %H:%M:%S")}'.encode('utf-8'),
                ordering_key='direct-ip-test',
                connection_type='direct_ip',
                test_timestamp=str(int(time.time()))
            )
            
            print("⏳ Waiting for publish result...")
            try:
                message_id = future.result(timeout=15)
                print(f"🎉 SUCCESS! Message published successfully!")
                print(f"   Message ID: {message_id}")
                print(f"   Used direct IP: 10.128.0.26:9092")
                return True
                
            except Exception as e:
                print(f"❌ Message publish failed: {e}")
                return False
                
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you have the required dependencies installed")
        return False
    except Exception as e:
        print(f"❌ MSAK test error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_msak_hostname():
    """Test MSAK with original hostname for comparison."""
    print("\n=== MSAK Hostname Test (for comparison) ===")
    
    try:
        from google.cloud.pubsublite.transport import MSAKConfig
        from google.cloud.pubsublite.cloudpubsub.enhanced_publisher_client import EnhancedPublisherClient
        from google.cloud.pubsub_v1.types import BatchSettings
        
        # Use original hostname
        msak_config = MSAKConfig(
            project_id='ygnahz-eg-codelab',
            location='us-central1',
            cluster_id='testpsl'
            # No endpoint_override - use default hostname
        )
        
        print(f"Testing with hostname: {msak_config.bootstrap_servers}")
        
        # Create client
        client = EnhancedPublisherClient.create_for_msak(
            topic='testtopic',
            msak_config=msak_config,
            per_partition_batching_settings=BatchSettings(max_messages=3)
        )
        
        print("✅ Client created successfully!")
        
        # Test publishing with timeout
        with client:
            print("📤 Publishing test message...")
            
            future = client.publish(
                topic='testtopic',
                data=b'Test message with hostname',
                ordering_key='hostname-test'
            )
            
            print("⏳ Waiting for publish result (10s timeout)...")
            try:
                message_id = future.result(timeout=10)
                print(f"🎉 SUCCESS! Hostname method also works!")
                print(f"   Message ID: {message_id}")
                return True
                
            except Exception as e:
                print(f"❌ Hostname method failed (as expected): {e}")
                return False
                
    except Exception as e:
        print(f"❌ Hostname test error: {e}")
        return False

def main():
    print("🚀 MSAK Direct IP Connection Test")
    print("This script tests MSAK connectivity using direct IP instead of hostname")
    print("=" * 60)
    
    # Clear environment variables
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    # Run DNS tests
    test_dns_resolution()
    
    # Test direct IP method
    direct_ip_success = test_msak_direct_ip()
    
    # Test hostname method for comparison
    hostname_success = test_msak_hostname()
    
    # Summary
    print("\n" + "=" * 60)
    print("🏁 Test Summary:")
    print(f"   Direct IP method: {'✅ SUCCESS' if direct_ip_success else '❌ FAILED'}")
    print(f"   Hostname method:  {'✅ SUCCESS' if hostname_success else '❌ FAILED'}")
    
    if direct_ip_success and not hostname_success:
        print("\n💡 Recommendation:")
        print("   Use endpoint_override with direct IP (10.128.0.26:9092)")
        print("   Or fix DNS resolution for the MSAK hostname")
    elif direct_ip_success and hostname_success:
        print("\n🎉 Both methods work! DNS resolution is properly configured.")
    elif not direct_ip_success:
        print("\n🔍 Neither method works. Check authentication and network connectivity.")
    
    return 0 if direct_ip_success else 1

if __name__ == "__main__":
    sys.exit(main())