#!/usr/bin/env python

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

"""
Test AdminClient using Application Default Credentials (ADC).

This uses google.auth.default() instead of a custom TokenProvider,
which is the standard way to authenticate with Google Cloud.
"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
    from confluent_kafka.admin import AdminClient
    import google.auth
    from google.auth.transport.requests import Request
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure google-auth and confluent-kafka are installed")
    exit(1)


def oauth_callback_with_default_creds(oauth_config):
    """OAuth callback using Application Default Credentials."""
    try:
        # Get default credentials
        credentials, project = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )

        # Refresh if needed
        if not credentials.valid:
            if credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())

        # Set the token
        oauth_config.token = credentials.token

        print(f"   [OAuth] Token obtained from ADC (project: {project})")

    except Exception as e:
        print(f"   [OAuth] ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


def test_admin_with_default_creds(bootstrap_servers: str):
    """Test Kafka AdminClient with Application Default Credentials.

    Args:
        bootstrap_servers: Kafka bootstrap servers
    """
    print("\n=== Testing Kafka AdminClient with ADC ===\n")

    print("1. Checking for Application Default Credentials")
    try:
        credentials, project = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        print(f"   ✓ Credentials found")
        print(f"   Project: {project}")
        print(f"   Credential type: {type(credentials).__name__}\n")
    except Exception as e:
        print(f"   ✗ No credentials found: {e}")
        print("\n   Run: gcloud auth application-default login")
        return

    print("2. Creating Kafka AdminClient with ADC")
    admin_config = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': oauth_callback_with_default_creds,
        'socket.timeout.ms': 60000,
        'request.timeout.ms': 30000,
        'api.version.request.timeout.ms': 10000,
    }

    try:
        admin_client = AdminClient(admin_config)
        print("   ✓ AdminClient created\n")
    except Exception as e:
        print(f"   ✗ Failed to create AdminClient: {e}")
        import traceback
        traceback.print_exc()
        return

    print("3. Testing list_topics()")
    try:
        print("   Calling list_topics()...")
        metadata = admin_client.list_topics(timeout=30)

        print(f"   ✓ Successfully retrieved metadata")
        print(f"   Total topics: {len(metadata.topics)}\n")

        print("   Topics found:")
        for topic_name, topic_metadata in metadata.topics.items():
            if not topic_name.startswith('_'):
                print(f"     - {topic_name}")
                print(f"       Partitions: {len(topic_metadata.partitions)}")

    except Exception as e:
        print(f"   ✗ Failed to list topics: {e}")
        print(f"   Exception type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return

    print("\n=== Test Complete ===\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Test Kafka AdminClient with Application Default Credentials"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092",
        help="Kafka bootstrap servers"
    )

    args = parser.parse_args()

    print("\nMake sure you've run: gcloud auth application-default login\n")

    test_admin_with_default_creds(
        bootstrap_servers=args.bootstrap_servers,
    )
