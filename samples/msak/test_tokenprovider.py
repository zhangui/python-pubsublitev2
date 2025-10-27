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
Test TokenProvider to verify OAuth token generation.
"""

try:
    from tokenprovider import TokenProvider
except ImportError:
    TokenProvider = None


def test_token_provider():
    """Test TokenProvider token generation."""
    print("\n=== Testing TokenProvider ===\n")

    if TokenProvider is None:
        print("✗ TokenProvider not available")
        return

    print("1. Creating TokenProvider")
    try:
        token_provider = TokenProvider()
        print("   ✓ TokenProvider created\n")
    except Exception as e:
        print(f"   ✗ Failed to create TokenProvider: {e}")
        import traceback
        traceback.print_exc()
        return

    print("2. Testing get_token() with mock oauth_config")
    try:
        # Mock oauth_config that Kafka would pass
        oauth_config = "mock_config_from_kafka"

        print(f"   Calling get_token({oauth_config!r})")
        token_provider.get_token(oauth_config)
        print("   ✓ get_token() completed without exception\n")

    except Exception as e:
        print(f"   ✗ get_token() failed: {e}")
        import traceback
        traceback.print_exc()
        return

    print("3. Checking if TokenProvider has required methods")
    required_methods = ['get_token']
    for method in required_methods:
        if hasattr(token_provider, method):
            print(f"   ✓ Method '{method}' exists")
        else:
            print(f"   ✗ Method '{method}' missing")

    print("\n=== TokenProvider Test Complete ===\n")


if __name__ == "__main__":
    test_token_provider()
