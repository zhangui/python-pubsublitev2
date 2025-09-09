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

import os
import unittest
from unittest import mock
from concurrent.futures import Future

from google.cloud.pubsublite.cloudpubsub.universal_client import UniversalPublisherClient
from google.cloud.pubsublite.types import TopicPath

class TestUniversalPublisherClient(unittest.TestCase):
    
    def setUp(self):
        self.topic_path = TopicPath.parse("projects/test-project/locations/us-central1-a/topics/test-topic")
        self.test_data = b"test message data"
        self.test_ordering_key = "test-key"
        self.test_attrs = {"attr1": "value1", "attr2": "value2"}

    def test_default_backend_pubsublite(self):
        """Test that Pub/Sub Lite is the default backend."""
        with mock.patch.dict(os.environ, {}, clear=True):
            client = UniversalPublisherClient()
            self.assertFalse(client._use_kafka)
            self.assertEqual(client.backend_type, "pubsublite")

    def test_explicit_pubsublite_backend(self):
        """Test explicitly setting Pub/Sub Lite backend."""
        client = UniversalPublisherClient(use_kafka=False)
        self.assertFalse(client._use_kafka)
        self.assertEqual(client.backend_type, "pubsublite")

    @mock.patch('google.auth.default')
    def test_explicit_kafka_backend(self, mock_default):
        """Test explicitly setting Kafka backend."""
        from google.cloud.pubsublite.cloudpubsub.msak_client import KafkaConfig
        
        # Mock credentials
        mock_creds = mock.MagicMock()
        mock_default.return_value = (mock_creds, 'test-project')
        
        kafka_config = KafkaConfig(
            bootstrap_servers=['localhost:9092'],
            auth_endpoint='localhost:14293',
            credentials=mock_creds,  # Provide mock credentials
        )
        
        client = UniversalPublisherClient(use_kafka=True, kafka_config=kafka_config)
        self.assertTrue(client._use_kafka)
        self.assertEqual(client.backend_type, "kafka")

    @mock.patch('google.auth.default')
    def test_environment_variable_kafka_true(self, mock_default):
        """Test using Kafka backend via environment variable."""
        from google.cloud.pubsublite.cloudpubsub.msak_client import KafkaConfig
        
        # Mock credentials
        mock_creds = mock.MagicMock()
        mock_default.return_value = (mock_creds, 'test-project')
        
        kafka_config = KafkaConfig(
            bootstrap_servers=['localhost:9092'],
            auth_endpoint='localhost:14293',
            credentials=mock_creds,  # Provide mock credentials
        )
        
        with mock.patch.dict(os.environ, {'PUBSUBLITE_USE_KAFKA': 'true'}):
            client = UniversalPublisherClient(kafka_config=kafka_config)
            self.assertTrue(client._use_kafka)
            self.assertEqual(client.backend_type, "kafka")

    def test_environment_variable_kafka_false(self):
        """Test using Pub/Sub Lite backend via environment variable."""
        with mock.patch.dict(os.environ, {'PUBSUBLITE_USE_KAFKA': 'false'}):
            client = UniversalPublisherClient()
            self.assertFalse(client._use_kafka)
            self.assertEqual(client.backend_type, "pubsublite")

    def test_kafka_without_config_raises_error(self):
        """Test that using Kafka without config raises ValueError."""
        with self.assertRaises(ValueError):
            client = UniversalPublisherClient(use_kafka=True)
            with client:
                pass

    @mock.patch('google.cloud.pubsublite.cloudpubsub.universal_client.PublisherClient')
    def test_pubsublite_backend_delegation(self, mock_pubsublite_client):
        """Test that calls are properly delegated to Pub/Sub Lite backend."""
        # Setup mock
        mock_instance = mock_pubsublite_client.return_value
        mock_instance.__enter__.return_value = mock_instance
        mock_instance.__exit__.return_value = None
        
        future = Future()
        future.set_result("test-ack-id")
        mock_instance.publish.return_value = future
        
        # Test client
        client = UniversalPublisherClient(use_kafka=False)
        
        with client:
            result_future = client.publish(
                topic=self.topic_path,
                data=self.test_data,
                ordering_key=self.test_ordering_key,
                **self.test_attrs
            )
            
            # Verify delegation
            mock_instance.publish.assert_called_once_with(
                topic=self.topic_path,
                data=self.test_data,
                ordering_key=self.test_ordering_key,
                **self.test_attrs
            )
            
            # Verify result
            self.assertEqual(result_future.result(), "test-ack-id")

    @mock.patch('google.cloud.pubsublite.cloudpubsub.universal_client.MsakClient')
    @mock.patch('google.auth.default')
    def test_kafka_backend_delegation(self, mock_default, mock_kafka_client):
        """Test that calls are properly delegated to Kafka backend."""
        from google.cloud.pubsublite.cloudpubsub.msak_client import KafkaConfig
        
        # Mock credentials
        mock_creds = mock.MagicMock()
        mock_default.return_value = (mock_creds, 'test-project')
        
        # Setup mock
        mock_instance = mock_kafka_client.return_value
        mock_instance.__enter__.return_value = mock_instance
        mock_instance.__exit__.return_value = None
        
        future = Future()
        future.set_result("test-topic:0:12345")
        mock_instance.publish.return_value = future
        
        # Test client
        kafka_config = KafkaConfig(
            bootstrap_servers=['localhost:9092'],
            auth_endpoint='localhost:14293',
            credentials=mock_creds,
        )
        
        client = UniversalPublisherClient(use_kafka=True, kafka_config=kafka_config)
        
        with client:
            result_future = client.publish(
                topic=self.topic_path,
                data=self.test_data,
                ordering_key=self.test_ordering_key,
                **self.test_attrs
            )
            
            # Verify delegation
            mock_instance.publish.assert_called_once_with(
                topic=self.topic_path,
                data=self.test_data,
                ordering_key=self.test_ordering_key,
                **self.test_attrs
            )
            
            # Verify result
            self.assertEqual(result_future.result(), "test-topic:0:12345")

    def test_publish_without_enter_fails(self):
        """Test that publishing without entering context fails."""
        client = UniversalPublisherClient(use_kafka=False)
        
        future = client.publish(
            topic=self.topic_path,
            data=self.test_data,
        )
        
        with self.assertRaises(RuntimeError):
            future.result()


if __name__ == '__main__':
    unittest.main()