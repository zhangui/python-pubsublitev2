# Google Managed Service for Apache Kafka Samples

This directory contains samples for using the Universal Publisher Client with Google Managed Service for Apache Kafka backend.

## Prerequisites

1. **Install dependencies with Kafka support:**
   ```bash
   pip install google-cloud-pubsublite[kafka]
   ```

2. **Set up Google Cloud authentication:**
   ```bash
   # Using gcloud (recommended for development)
   gcloud auth application-default login
   
   # Or set up a service account
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
   ```

3. **Set up local OAuth authentication server** (required for Kafka authentication):
   
   Follow the [Google Cloud documentation](https://cloud.google.com/managed-service-for-apache-kafka/docs/quickstart-python) to set up the local authentication server:
   
   ```bash
   # Clone the authentication server
   git clone https://github.com/googleapis/python-pubsub
   cd python-pubsub/samples/snippets/kafka-auth-local-server
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Start the authentication server
   python main.py --port 14293
   ```

4. **Create a Kafka cluster and topic** in Google Cloud Console.

## Running the Samples

### Publish Single Message

Publishes one message to a Kafka topic:

```bash
python publish_single_message.py \
    --project-id YOUR_PROJECT_ID \
    --location YOUR_LOCATION \
    --topic-name YOUR_TOPIC_NAME \
    --bootstrap-servers YOUR_BOOTSTRAP_SERVER_1:9092,YOUR_BOOTSTRAP_SERVER_2:9092
```

#### Example:
```bash
python publish_single_message.py \
    --project-id my-project-123 \
    --location us-central1-a \
    --topic-name my-kafka-topic \
    --bootstrap-servers kafka-server1.example.com:9092,kafka-server2.example.com:9092 \
    --message "Hello from Python!"
```

#### Optional parameters:
- `--auth-endpoint`: OAuth server endpoint (default: localhost:14293)
- `--message`: Custom message content

## Sample Output

```
============================================================
Google Managed Service for Apache Kafka - Publish Message
============================================================
Publishing to Kafka topic: my-kafka-topic
Bootstrap servers: kafka-server1.example.com:9092,kafka-server2.example.com:9092
Message: Hello from Python!
Using backend: kafka
✓ Message published successfully!
  Ack ID: my-kafka-topic:0:12345
  Topic: my-kafka-topic
  Partition: 0
  Offset: 12345
============================================================
Message publishing completed successfully!
```

## Key Features

- **Unified API**: Uses the same interface as Pub/Sub Lite clients
- **Automatic Authentication**: Leverages Google Cloud Application Default Credentials
- **Error Handling**: Comprehensive error handling with clear messages
- **Kafka Integration**: Seamless integration with Google Managed Service for Apache Kafka
- **Message Attributes**: Support for custom attributes as Kafka headers
- **Ordering Keys**: Support for partition assignment via ordering keys

## Troubleshooting

### ImportError: confluent-kafka not found
Install the Kafka optional dependencies:
```bash
pip install google-cloud-pubsublite[kafka]
```

### Authentication errors
1. Ensure the local OAuth server is running on the correct port
2. Verify your Google Cloud credentials are set up correctly
3. Check that your service account has the necessary permissions

### Connection errors
1. Verify the bootstrap server addresses are correct
2. Ensure your Kafka cluster is running and accessible
3. Check network connectivity and firewall rules

## Related Documentation

- [Google Managed Service for Apache Kafka Documentation](https://cloud.google.com/managed-service-for-apache-kafka/docs)
- [Python Client Quickstart](https://cloud.google.com/managed-service-for-apache-kafka/docs/quickstart-python)
- [Universal Publisher Client Documentation](../../google/cloud/pubsublite/cloudpubsub/universal_client.py)