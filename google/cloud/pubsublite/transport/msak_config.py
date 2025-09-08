# Copyright 2020 Google LLC
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

from typing import Optional
from dataclasses import dataclass

from google.auth.credentials import Credentials


@dataclass
class MSAKConfig:
    """Configuration for Google Managed Service for Apache Kafka.
    
    Attributes:
        project_id: Google Cloud project ID containing the MSAK cluster.
        location: Google Cloud location/region of the MSAK cluster.
        cluster_id: The MSAK cluster identifier.
        credentials: Google Cloud credentials for authentication.
        endpoint_override: Optional custom endpoint for testing.
    """
    project_id: str
    location: str
    cluster_id: str
    credentials: Optional[Credentials] = None
    endpoint_override: Optional[str] = None
    
    @property
    def cluster_path(self) -> str:
        """Get the full cluster resource path."""
        return f"projects/{self.project_id}/locations/{self.location}/clusters/{self.cluster_id}"
    
    @property
    def bootstrap_servers(self) -> str:
        """Get the bootstrap servers endpoint for this MSAK cluster."""
        if self.endpoint_override:
            return self.endpoint_override
        
        # Google MSAK bootstrap server format
        return f"bootstrap.{self.cluster_id}.{self.location}.managedkafka.{self.project_id}.cloud.goog:9092"