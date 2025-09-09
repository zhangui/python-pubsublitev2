import argparse
import time
from datetime import datetime
from uuid import uuid4

import confluent_kafka
from confluent_kafka import Consumer, KafkaError, TopicPartition

from tokenprovider import TokenProvider

parser = argparse.ArgumentParser()
parser.add_argument('-b', '--bootstrap-servers', dest='bootstrap', type=str, required=True)
parser.add_argument('-t', '--topic-name', dest='topic_name', type=str, default='example-topic')
parser.add_argument('-n', '--num_messages', dest='num_messages', type=int, default=3)
parser.add_argument('--poll_timeout_s', type=float, default=0.2, help='Per-poll timeout seconds')
parser.add_argument('--overall_timeout_s', type=float, default=10.0, help='Overall timeout to stop polling')
args = parser.parse_args()

token_provider = TokenProvider()

config = {
    'bootstrap.servers': args.bootstrap,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': token_provider.get_token,

    # Consumer specifics
    'group.id': f'tail-consumer-{uuid4()}',
    'enable.partition.eof': True,
    'auto.offset.reset': 'latest',
}

consumer = Consumer(config)

try:
    # Discover partitions for the topic
    md = consumer.list_topics(args.topic_name, timeout=10)
    if args.topic_name not in md.topics:
        raise RuntimeError(f"Topic '{args.topic_name}' not found")

    partitions = list(md.topics[args.topic_name].partitions.keys())
    if not partitions:
        raise RuntimeError(f"Topic '{args.topic_name}' has no partitions")

    # For each partition: compute start offset = max(low, high - n)
    seek_tps = []
    for p in partitions:
        tp = TopicPartition(args.topic_name, p)
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        start = max(low, high - args.num_messages)  # small window near the tail
        seek_tps.append(TopicPartition(args.topic_name, p, start))

    # Directly assign (no subscribe) and seek to computed offsets
    consumer.assign(seek_tps)

    # Optional: explicitly seek in case broker adjusted offsets
    for tp in seek_tps:
        consumer.seek(tp)

    # We'll read until we hit EOF on all partitions or run out of time
    eof_seen = set()
    collected = []
    deadline = time.time() + args.overall_timeout_s

    while time.time() < deadline and len(eof_seen) < len(partitions):
        msg = consumer.poll(args.poll_timeout_s)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof_seen.add(msg.partition())
                continue
            else:
                # Non-EOF error: log and continue
                print(f"[Error] {msg.error()}")
                continue

        collected.append(msg)

    if not collected:
        print("(No messages found in the requested window.)")
    else:
        # Sort by Kafka timestamp (ms since epoch), newest first
        collected.sort(key=lambda m: (m.timestamp()[1] or 0), reverse=True)
        out = collected[:args.num_messages]

        for m in out:
            ts_type, ts_ms = m.timestamp()
            ts_str = datetime.utcfromtimestamp((ts_ms or 0) / 1000.0).isoformat() + "Z"
            key = m.key().decode('utf-8', errors='ignore') if m.key() else None
            val = m.value().decode('utf-8', errors='ignore') if m.value() else None
            print(
                f"[{ts_str}] {m.topic()}[{m.partition()}]@{m.offset()} "
                f"key={key!r} value={val!r}"
            )

finally:
    consumer.close()
