#!/usr/bin/env python3
"""Official Google MSAK Python Producer - based on quickstart-python docs."""

import confluent_kafka
import argparse
from tokenprovider import TokenProvider

def main():
    parser = argparse.ArgumentParser(description='MSAK Python Producer')
    parser.add_argument('-b', '--bootstrap-servers', dest='bootstrap', type=str, required=True,
                       help='Bootstrap servers (e.g., bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092)')
    parser.add_argument('-t', '--topic-name', dest='topic_name', type=str, default='testtopic', required=False,
                       help='Topic name (default: testtopic)')
    parser.add_argument('-n', '--num_messages', dest='num_messages', type=int, default=5, required=False,
                       help='Number of messages to send (default: 5)')
    args = parser.parse_args()

    print(f"🚀 MSAK Python Producer")
    print(f"📡 Bootstrap servers: {args.bootstrap}")
    print(f"📝 Topic: {args.topic_name}")
    print(f"📊 Messages to send: {args.num_messages}")

    # Create token provider for Google Cloud authentication
    print("🔐 Initializing Google Cloud authentication...")
    token_provider = TokenProvider()

    # Producer configuration - exactly like Google's official example
    config = {
        'bootstrap.servers': args.bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
    }

    print("🔧 Creating Kafka producer...")
    print(f"📋 Producer config:")
    for key, value in config.items():
        if key == 'oauth_cb':
            print(f"   {key}: <TokenProvider.get_token>")
        else:
            print(f"   {key}: {value}")
    
    producer = confluent_kafka.Producer(config)
    print("✅ Producer created, testing connection...")

    # Delivery callback
    messages_delivered = 0
    def callback(error, message):
        nonlocal messages_delivered
        if error is not None:
            print(f"❌ Delivery error: {error}")
            return
        messages_delivered += 1
        print(f"✅ Message {messages_delivered} delivered to {message.topic()}[{message.partition()}] at offset {message.offset()}")

    print(f"\n📤 Sending {args.num_messages} messages...")
    
    # Send messages - exactly like Google's example
    for i in range(args.num_messages):
        message = f"{i} hello world from Python MSAK producer!".encode('utf-8')
        try:
            producer.produce(args.topic_name, message, callback=callback)
        except Exception as e:
            print(f"❌ Failed to produce message {i}: {e}")

    print("⏳ Waiting for all messages to be delivered...")
    producer.flush()
    
    print(f"\n📊 Summary: {messages_delivered}/{args.num_messages} messages delivered successfully")
    
    if messages_delivered == args.num_messages:
        print("🎉 All messages delivered successfully!")
        return 0
    else:
        print(f"⚠️  {args.num_messages - messages_delivered} messages failed to deliver")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())