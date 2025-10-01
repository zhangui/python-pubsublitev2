from google.cloud import pubsublite_v1

producer_config = {
    'bootstrap.servers': 'test:9092',
}

client = pubsublite_v1.PublisherServiceClient(
    transport='kafka',
    producer_config=producer_config,
)
print('SUCCESS: Client created')

# 1. Clear Python bytecode cache for publisher_service
  find google/cloud/pubsublite_v1/services/publisher_service -name "*.pyc" -delete
  find google/cloud/pubsublite_v1/services/publisher_service -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null

  # 2. Clear all Python bytecode cache in the entire project (more thorough)
  find /Users/yangzhang/Desktop/psl/python-pubsublite -name "*.pyc" -delete
  find /Users/yangzhang/Desktop/psl/python-pubsublite -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null

find . -type f -name "*.pyc" -delete && find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null