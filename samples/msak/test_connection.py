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
Test basic connectivity to Kafka broker.

This tests if we can even reach the broker at the TCP level.
"""

import socket
import ssl


def test_connection(bootstrap_servers: str):
    """Test basic TCP and SSL connectivity to Kafka broker.

    Args:
        bootstrap_servers: Kafka bootstrap servers (host:port)
    """
    print("\n=== Testing Broker Connectivity ===\n")

    # Parse host and port
    if ':' in bootstrap_servers:
        host, port_str = bootstrap_servers.split(':', 1)
        port = int(port_str)
    else:
        host = bootstrap_servers
        port = 9092

    print(f"Target: {host}:{port}\n")

    # Test 1: DNS resolution
    print("1. Testing DNS resolution")
    try:
        ip_address = socket.gethostbyname(host)
        print(f"   ✓ DNS resolved: {host} -> {ip_address}\n")
    except socket.gaierror as e:
        print(f"   ✗ DNS resolution failed: {e}\n")
        return

    # Test 2: TCP connection
    print("2. Testing TCP connection")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((host, port))
        print(f"   ✓ TCP connection successful\n")
        sock.close()
    except Exception as e:
        print(f"   ✗ TCP connection failed: {e}\n")
        return

    # Test 3: SSL/TLS connection
    print("3. Testing SSL/TLS connection")
    try:
        context = ssl.create_default_context()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        ssl_sock = context.wrap_socket(sock, server_hostname=host)
        ssl_sock.connect((host, port))
        print(f"   ✓ SSL/TLS connection successful")
        print(f"   Protocol: {ssl_sock.version()}")
        print(f"   Cipher: {ssl_sock.cipher()}\n")
        ssl_sock.close()
    except Exception as e:
        print(f"   ✗ SSL/TLS connection failed: {e}\n")
        return

    print("=== All connectivity tests passed ===\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Test basic connectivity to Kafka broker"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog:9092",
        help="Kafka bootstrap servers"
    )

    args = parser.parse_args()

    test_connection(args.bootstrap_servers)
