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

from typing import Dict, Any, Optional
from google.auth.credentials import Credentials


class KafkaConfig:
    """Configuration for Kafka client connection."""
    
    def __init__(
        self,
        credentials: Optional[Credentials] = None,
        producer_config: Optional[Dict[str, Any]] = None,
    ):
        self.credentials = credentials
        self.producer_config = producer_config or {}


def create_default_kafka_producer_config() -> Dict[str, Any]:
    """Create default Kafka producer configuration optimized for Google Cloud."""
    return {
        # Performance settings
        "acks": "all",  # Ensure all replicas acknowledge
    }