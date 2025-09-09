# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest import mock
from concurrent.futures import Future

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.pubsublite.types import TopicPath

# Mock confluent_kafka to avoid import errors in CI
mock_confluent_kafka = mock.MagicMock()
mock_producer = mock.MagicMock()
mock_kafka_error = mock.MagicMock()
mock_confluent_kafka.Producer = mock_producer
mock_confluent_kafka.KafkaError = mock_kafka_error

with mock.patch.dict('sys.modules', {'confluent_kafka': mock_confluent_kafka}):
    from google.cloud.pubsublite.cloudpubsub.msak_client import MsakClient, KafkaConfig


class TestMsakClient(unittest.TestCase):

    def setUp(self):
        self.topic_path = TopicPath.parse("projects/test-project/locations/us-central1-a/topics/test-topic")
        self.test_data = b"test message data"
        self.test_ordering_key = "test-key"
        self.test_attrs = {"attr1": "value1", "attr2": "value2"}
        
        self.kafka_config = KafkaConfig(
            bootstrap_servers=['localhost:9092'],
            auth_endpoint='localhost:14293',
            producer_config={'test.config': 'value'}
        )

    def test_kafka_config_creation(self):
        """Test KafkaConfig creation and properties."""
        config = KafkaConfig(
            bootstrap_servers=['server1:9092', 'server2:9092'],
            auth_endpoint='localhost:14293',
            producer_config={'custom.setting': 'value'}
        )
        
        self.assertEqual(config.bootstrap_servers, ['server1:9092', 'server2:9092'])
        self.assertEqual(config.auth_endpoint, 'localhost:14293')
        self.assertEqual(config.producer_config['custom.setting'], 'value')

    def test_topic_path_conversion(self):
        """Test conversion from TopicPath to Kafka topic name."""
        client = MsakClient(self.kafka_config)
        
        # Test TopicPath conversion
        kafka_topic = client._topic_path_to_kafka_topic(self.topic_path)
        self.assertEqual(kafka_topic, "test-topic")
        
        # Test string conversion 
        kafka_topic = client._topic_path_to_kafka_topic("projects/test/locations/us-central1-a/topics/my-topic")
        self.assertEqual(kafka_topic, "my-topic")

    def test_create_kafka_message(self):
        """Test Kafka message creation from Pub/Sub Lite format."""
        client = MsakClient(self.kafka_config)
        
        message = client._create_kafka_message(
            data=self.test_data,
            ordering_key=self.test_ordering_key,
            **self.test_attrs
        )
        
        self.assertEqual(message['value'], self.test_data)
        self.assertEqual(message['key'], self.test_ordering_key.encode('utf-8'))
        self.assertEqual(message['headers']['attr1'], b'value1')
        self.assertEqual(message['headers']['attr2'], b'value2')

    def test_create_kafka_message_no_ordering_key(self):
        """Test Kafka message creation without ordering key."""
        client = MsakClient(self.kafka_config)
        
        message = client._create_kafka_message(data=self.test_data)
        
        self.assertEqual(message['value'], self.test_data)
        self.assertNotIn('key', message)
        self.assertNotIn('headers', message)

    def test_producer_config_creation(self):
        """Test Kafka producer configuration generation."""
        client = MsakClient(self.kafka_config)
        
        config = client._create_producer_config()
        
        # Check required OAuth settings
        self.assertEqual(config['bootstrap.servers'], 'localhost:9092')
        self.assertEqual(config['security.protocol'], 'SASL_SSL')
        self.assertEqual(config['sasl.mechanisms'], 'OAUTHBEARER')
        self.assertEqual(config['sasl.oauthbearer.token.endpoint.url'], 'localhost:14293')
        
        # Check performance settings
        self.assertEqual(config['batch.size'], 16384)
        self.assertEqual(config['acks'], 'all')
        
        # Check custom config is applied
        self.assertEqual(config['test.config'], 'value')

    @mock.patch('google.cloud.pubsublite.cloudpubsub.msak_client.Producer')
    def test_context_manager_lifecycle(self, mock_producer_class):
        """Test proper context manager lifecycle."""
        mock_producer_instance = mock.MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        client = MsakClient(self.kafka_config)
        self.assertFalse(client._started)
        
        with client:
            self.assertTrue(client._started)
            mock_producer_class.assert_called_once()
            
        mock_producer_instance.flush.assert_called_once()
        self.assertFalse(client._started)

    @mock.patch('google.cloud.pubsublite.cloudpubsub.msak_client.Producer')
    def test_publish_success(self, mock_producer_class):
        """Test successful message publishing."""
        mock_producer_instance = mock.MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        # Setup successful callback
        def mock_produce(**kwargs):
            callback = kwargs['callback']
            # Simulate successful delivery
            mock_msg = mock.MagicMock()
            mock_msg.topic.return_value = "test-topic"
            mock_msg.partition.return_value = 0
            mock_msg.offset.return_value = 12345
            callback(None, mock_msg)  # err=None means success
            
        mock_producer_instance.produce.side_effect = mock_produce
        
        client = MsakClient(self.kafka_config)
        
        with client:
            future = client.publish(
                topic=self.topic_path,
                data=self.test_data,
                ordering_key=self.test_ordering_key,
                **self.test_attrs
            )
            
            result = future.result()
            self.assertEqual(result, "test-topic:0:12345")
            
            # Verify produce was called with correct arguments
            mock_producer_instance.produce.assert_called_once()
            call_kwargs = mock_producer_instance.produce.call_args[1]
            self.assertEqual(call_kwargs['topic'], 'test-topic')
            self.assertEqual(call_kwargs['value'], self.test_data)

    @mock.patch('google.cloud.pubsublite.cloudpubsub.msak_client.Producer')
    def test_publish_error(self, mock_producer_class):
        """Test error handling during message publishing."""
        mock_producer_instance = mock.MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        # Setup error callback
        def mock_produce(**kwargs):
            callback = kwargs['callback']
            # Simulate error
            mock_error = mock.MagicMock()
            mock_error.code.return_value = 'TEST_ERROR'
            callback(mock_error, None)  # err is not None
            
        mock_producer_instance.produce.side_effect = mock_produce
        
        client = MsakClient(self.kafka_config)
        
        with client:
            future = client.publish(
                topic=self.topic_path,
                data=self.test_data,
            )
            
            with self.assertRaises(GoogleAPICallError):
                future.result()

    def test_publish_without_start_fails(self):
        """Test that publishing without starting client fails."""
        client = MsakClient(self.kafka_config)
        
        future = client.publish(
            topic=self.topic_path,
            data=self.test_data,
        )
        
        with self.assertRaises(GoogleAPICallError):
            future.result()


if __name__ == '__main__':
    unittest.main()