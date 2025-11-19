# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
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
#
from collections import OrderedDict
from typing import Dict, Type

from .base import CursorServiceTransport
from .grpc import CursorServiceGrpcTransport
from .grpc_asyncio import CursorServiceGrpcAsyncIOTransport

# Define HAS_KAFKA to conditionally import Kafka transport
try:
    from .kafka import CursorServiceKafkaTransport
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False


# Compile a registry of transports.
_transport_registry = OrderedDict()  # type: Dict[str, Type[CursorServiceTransport]]
_transport_registry["grpc"] = CursorServiceGrpcTransport
_transport_registry["grpc_asyncio"] = CursorServiceGrpcAsyncIOTransport

if HAS_KAFKA:
    _transport_registry["kafka"] = CursorServiceKafkaTransport

__all__ = (
    "CursorServiceTransport",
    "CursorServiceGrpcTransport",
    "CursorServiceGrpcAsyncIOTransport",
    "CursorServiceKafkaTransport" if HAS_KAFKA else None,
)

# Filter out None values in __all__
__all__ = tuple(x for x in __all__ if x is not None)
