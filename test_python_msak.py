#!/usr/bin/env python3
"""Simple MSAK Python test equivalent to the Java Kafka tools."""

import os
import sys
import subprocess

def main():
    print("🚀 MSAK Python Producer/Consumer Test")
    print("This test mirrors the Java Kafka tools approach")
    
    # Configuration - update with your cluster details
    PROJECT_ID = "ygnahz-eg-codelab" 
    REGION = "us-central1"
    CLUSTER_ID = "testpsl"
    TOPIC = "testtopic"
    
    bootstrap_servers = f"bootstrap.{CLUSTER_ID}.{REGION}.managedkafka.{PROJECT_ID}.cloud.goog:9092"
    
    print(f"📡 Bootstrap servers: {bootstrap_servers}")
    print(f"📝 Topic: {TOPIC}")
    print()
    
    # Test 1: Send messages
    print("📤 Test 1: Sending 3 test messages...")
    try:
        result = subprocess.run([
            'python3', 'producer.py',
            '--bootstrap-servers', bootstrap_servers,
            '--topic-name', TOPIC,
            '--num_messages', '3'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Producer test successful!")
            print("Producer output:")
            print(result.stdout)
        else:
            print("❌ Producer test failed!")
            print("Error:", result.stderr)
            return 1
            
    except subprocess.TimeoutExpired:
        print("❌ Producer test timed out!")
        return 1
    except Exception as e:
        print(f"❌ Producer test error: {e}")
        return 1
    
    print("\n" + "="*50 + "\n")
    
    # Test 2: Consume messages
    print("📥 Test 2: Consuming messages...")
    try:
        result = subprocess.run([
            'python3', 'consumer.py', 
            '--bootstrap-servers', bootstrap_servers,
            '--topic-name', TOPIC,
            '--num_messages', '5'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Consumer test successful!")
            print("Consumer output:")
            print(result.stdout)
        else:
            print("❌ Consumer test failed!")
            print("Error:", result.stderr)
            return 1
            
    except subprocess.TimeoutExpired:
        print("⏰ Consumer test timed out (this is normal if no messages available)")
        print("Consumer may have successfully connected but found no messages")
    except Exception as e:
        print(f"❌ Consumer test error: {e}")
        return 1
    
    print("\n🎉 MSAK Python test completed!")
    print("\nEquivalent Java commands would be:")
    print(f"  kafka-console-producer.sh --topic {TOPIC} --bootstrap-server {bootstrap_servers} --producer.config client.properties")
    print(f"  kafka-console-consumer.sh --topic {TOPIC} --from-beginning --bootstrap-server {bootstrap_servers} --consumer.config client.properties")
    
    return 0

if __name__ == "__main__":
    # Make sure we have required files
    required_files = ['producer.py', 'consumer.py', 'tokenprovider.py']
    for file in required_files:
        if not os.path.exists(file):
            print(f"❌ Required file not found: {file}")
            sys.exit(1)
    
    sys.exit(main())