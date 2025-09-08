#!/usr/bin/env python3
"""Official Google MSAK Python Consumer - based on quickstart-python docs."""

import confluent_kafka
import argparse
from tokenprovider import TokenProvider

def main():
    parser = argparse.ArgumentParser(description='MSAK Python Consumer')
    parser.add_argument('-b', '--bootstrap-servers', dest='bootstrap', type=str, required=True,
                       help='Bootstrap servers (e.g., bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092)')
    parser.add_argument('-t', '--topic-name', dest='topic_name', type=str, default='testtopic', required=False,
                       help='Topic name (default: testtopic)')
    parser.add_argument('-g', '--group-id', dest='group_id', type=str, default='python-consumer-group', required=False,
                       help='Consumer group ID (default: python-consumer-group)')
    parser.add_argument('-n', '--num_messages', dest='num_messages', type=int, default=10, required=False,
                       help='Max number of messages to consume (default: 10)')
    args = parser.parse_args()

    print(f"🚀 MSAK Python Consumer")
    print(f"📡 Bootstrap servers: {args.bootstrap}")
    print(f"📝 Topic: {args.topic_name}")
    print(f"👥 Group ID: {args.group_id}")
    print(f"📊 Max messages: {args.num_messages}")

    # Create token provider for Google Cloud authentication
    print("🔐 Initializing Google Cloud authentication...")
    token_provider = TokenProvider()

    # Consumer configuration
    config = {
        'bootstrap.servers': args.bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
        'group.id': args.group_id,
        'auto.offset.reset': 'earliest',  # Start from beginning
        'enable.auto.commit': True,
    }

    print("🔧 Creating Kafka consumer...")
    consumer = confluent_kafka.Consumer(config)

    print(f"📥 Subscribing to topic: {args.topic_name}")
    consumer.subscribe([args.topic_name])

    print(f"\n👂 Consuming messages (press Ctrl+C to stop)...")
    
    messages_consumed = 0
    try:
        while messages_consumed < args.num_messages:
            message = consumer.poll(timeout=5.0)
            
            if message is None:
                print("⏳ No message received, waiting...")
                continue
                
            if message.error():
                if message.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"📍 Reached end of partition {message.partition()}")
                else:
                    print(f"❌ Consumer error: {message.error()}")
                continue
            
            # Process message
            messages_consumed += 1
            key = message.key().decode('utf-8') if message.key() else None
            value = message.value().decode('utf-8') if message.value() else None
            
            print(f"✅ Message {messages_consumed}:")
            print(f"   Topic: {message.topic()}")
            print(f"   Partition: {message.partition()}")
            print(f"   Offset: {message.offset()}")
            print(f"   Key: {key}")
            print(f"   Value: {value}")
            print()

    except KeyboardInterrupt:
        print(f"\n⏹️  Consumer interrupted by user")
    
    finally:
        print("🔒 Closing consumer...")
        consumer.close()
        
    print(f"\n📊 Summary: {messages_consumed} messages consumed")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())