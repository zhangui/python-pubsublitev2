from google.cloud import pubsublite_v1

producer_config = {
    'bootstrap.servers': 'test:9092',
}

client = pubsublite_v1.PublisherServiceClient(
    transport='kafka',
    producer_config=producer_config,
)
print('SUCCESS: Client created')
