import argparse
from datetime import datetime
from uuid import uuid4

import confluent_kafka
from confluent_kafka import Consumer, KafkaError
from tokenprovider import TokenProvider

parser = argparse.ArgumentParser()
parser.add_argument('-b', '--bootstrap-servers', dest='bootstrap', type=str, required=True)
parser.add_argument('-t', '--topic-name', dest='topic_name', type=str, default='example-topic')
parser.add_argument('--group-id', dest='group_id', type=str, default=f'cli-consumer-{uuid4()}')
parser.add_argument('--auto-offset-reset', dest='auto_offset_reset', type=str, default='latest',
                    choices=['earliest', 'latest', 'error'])
parser.add_argument('--poll-timeout', dest='poll_timeout', type=float, default=1.0,
                    help='poll timeout in seconds')
args = parser.parse_args()

token_provider = TokenProvider()

config = {
    'bootstrap.servers': args.bootstrap,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': token_provider.get_token,

    # Consumer config
    'group.id': args.group_id,
    'auto.offset.reset': args.auto_offset_reset,
    'enable.partition.eof': True,  # surface EOF events per partition
    # 'enable.auto.commit': True,  # default True; uncomment to be explicit
}

consumer = Consumer(config)

def fmt_ts(ts_ms: int | None) -> str:
    if not ts_ms:
        return "n/a"
    return datetime.utcfromtimestamp(ts_ms / 1000.0).isoformat() + "Z"

try:
    consumer.subscribe([args.topic_name])
    print(f"Consuming from topic '{args.topic_name}' "
          f"(group.id={args.group_id}, auto.offset.reset={args.auto_offset_reset})…")

    while True:
        msg = consumer.poll(args.poll_timeout)
        if msg is None:
            continue

        if msg.error():
            # Handle partition EOFs cleanly; log other errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                tp = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
                print(f"Reached EOF at {tp}")
                continue
            else:
                print(f"[Error] {msg.error()}")
                continue

        # Normal message
        ts_type, ts_ms = msg.timestamp()
        key = msg.key().decode('utf-8', errors='replace') if msg.key() else None
        val = msg.value().decode('utf-8', errors='replace') if msg.value() else None
        headers = msg.headers() or []

        print(
            f"[{fmt_ts(ts_ms)}] {msg.topic()}[{msg.partition()}]@{msg.offset()} "
            f"key={key!r} value={val!r} headers={headers}"
        )

except KeyboardInterrupt:
    print("\nStopping consumer…")
finally:
    consumer.close()
    print("Consumer closed.")
