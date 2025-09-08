# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

This project uses `nox` for task automation and testing. All commands should be run from the project root.

### Installation Options
- Basic installation: `pip install google-cloud-pubsublite`
- With MSAK support: `pip install google-cloud-pubsublite[msak]`
- All features: `pip install google-cloud-pubsublite[all]`

### Testing Commands
- `nox -s unit` - Run all unit tests across supported Python versions with multiple protobuf implementations
- `nox -s unit-3.12` - Run unit tests for specific Python version
- `nox -s unit-3.12 -- -k test_name` - Run a specific unit test
- `nox -s system` - Run system tests (requires GCP authentication)
- `nox -s system-3.8 -- -k test_name` - Run specific system test
- `nox -s cover` - Generate coverage report (requires 96%+ coverage to pass)

### Code Quality Commands
- `nox -s lint` - Run flake8 linting and black format checking
- `nox -s blacken` - Auto-format code with black
- `nox -s format` - Sort imports with isort then format with black
- `nox -s pytype` - Run static type checking
- `nox -s lint_setup_py` - Validate setup.py structure

### Documentation Commands
- `nox -s docs` - Build HTML documentation
- `nox -s docfx` - Build DocFX YAML documentation

### Sample Testing
Sample code is tested separately:
```bash
cd samples/snippets
nox -s py-3.8  # Run all sample tests
nox -s py-3.8 -- -k test_name  # Run specific sample test
```

## Architecture Overview

This is the Google Cloud Pub/Sub Lite Python client library with **dual transport support** for both Pub/Sub Lite and Google Managed Service for Apache Kafka (MSAK). The architecture consists of:

### Core Service Layers
1. **Generated GAPIC clients** (`google.cloud.pubsublite_v1.services.*`) - Auto-generated gRPC clients
2. **Transport abstraction layer** (`transport/`) - **NEW**: Unified interface for multiple backends
3. **Wire protocol layer** (`internal/wire/`) - Low-level Pub/Sub Lite streaming protocol
4. **Cloud Pub/Sub compatibility layer** (`cloudpubsub/`) - Familiar Pub/Sub-like API
5. **High-level clients** (`admin_client.py`, `cloudpubsub/publisher_client.py`, `enhanced_publisher_client.py`)

### Key Architectural Patterns

**Client Multiplexing**: Multiple logical clients share underlying connections via multiplexer classes:
- `MultiplexedPublisherClient` / `MultiplexedAsyncPublisherClient`
- `MultiplexedSubscriberClient` / `MultiplexedAsyncSubscriberClient`

**Wire Protocol Abstraction**: 
- `Connection` classes handle streaming gRPC connections
- `RetryingConnection` provides automatic retry with backoff
- Protocol-specific publishers/subscribers (`single_partition_publisher.py`, `subscriber_impl.py`)

**Cloud Pub/Sub Compatibility**: The `cloudpubsub/` module provides a familiar API that mirrors regular Cloud Pub/Sub while handling Pub/Sub Lite's partitioning model.

**Flow Control**: Built-in message flow control via `FlowControlSettings` and `flow_control_batcher.py`

### Transport Layer Architecture

**Transport Abstraction** (`transport/`):
- `transport_interface.py` - Abstract interfaces for all transport types
- `transport_factory.py` - Factory for creating transport instances
- `message_adapters.py` - Converts between Pub/Sub and Kafka message formats

**Pub/Sub Lite Transport** (`transport/pubsublite_*`):
- `pubsublite_publisher_transport.py` - Wraps existing wire layer publishers
- `pubsublite_subscriber_transport.py` - Wraps existing wire layer subscribers
- `pubsublite_admin_transport.py` - Wraps existing admin client

**MSAK Transport** (`transport/msak_*`):
- `msak_publisher_transport.py` - Google MSAK publisher using Cloud APIs
- `msak_subscriber_transport.py` - Google MSAK subscriber using Cloud APIs
- `msak_admin_transport.py` - Google MSAK admin using Cloud APIs
- `msak_config.py` - MSAK cluster configuration

### Key Components

**Publishers**:
- `PublisherClient` - High-level sync publisher (Pub/Sub Lite only)
- `EnhancedPublisherClient` - **NEW**: Unified publisher supporting both transports
- `AsyncPublisherClient` - High-level async publisher
- `routing_publisher.py` - Routes messages across partitions (Pub/Sub Lite)
- `partition_count_watching_publisher.py` - Dynamically handles partition changes (Pub/Sub Lite)

**Subscribers**:
- `SubscriberClient` - High-level sync subscriber (Pub/Sub Lite only)
- Future: `EnhancedSubscriberClient` - Unified subscriber supporting both transports
- `AsyncSubscriberClient` - High-level async subscriber  
- `assigning_subscriber.py` - Handles partition assignment (Pub/Sub Lite)
- `single_partition_subscriber.py` - Subscribes to individual partitions (Pub/Sub Lite)

**Administration**:
- `AdminClient` - Manages topics, subscriptions, and reservations (Pub/Sub Lite only)
- Transport layer provides admin operations for both Pub/Sub Lite and MSAK

### Testing Patterns

Tests follow the pattern `tests/unit/pubsublite/{module_path}/{file_name}_test.py`. Key patterns:
- Use of `_call_fut` ("Function Under Test") helper methods in unit tests
- `MUT` ("Module Under Test") variables for module references
- Extensive use of mocks for gRPC client testing
- Async test support with `pytest-asyncio`

### Type System
- Uses `types/` module for domain-specific types (`TopicPath`, `SubscriptionPath`, etc.)
- Heavy use of type annotations and `overrides` decorator for interface compliance
- Static type checking via `pytype`

### Error Handling
- Custom status code handling in `internal/status_codes.py`
- Permanent failure detection via `permanent_failable.py`
- Retry logic built into connection layers
- Transport-specific error mapping in `message_adapters.py`

## Usage Examples

### Pub/Sub Lite (Original)
```python
from google.cloud.pubsublite.types import TopicPath
from google.cloud.pubsublite.cloudpubsub import PublisherClient

topic_path = TopicPath.parse("projects/my-project/locations/us-central1/topics/my-topic")

with PublisherClient() as publisher:
    future = publisher.publish(topic_path, b"Hello Pub/Sub Lite!")
    message_id = future.result()
```

### Google MSAK (New)
```python
from google.cloud.pubsublite.transport import MSAKConfig
from google.cloud.pubsublite.cloudpubsub import EnhancedPublisherClient

msak_config = MSAKConfig(
    project_id="my-project",
    location="us-central1",
    cluster_id="my-cluster",
)

with EnhancedPublisherClient.create_for_msak("my-topic", msak_config) as publisher:
    future = publisher.publish("my-topic", b"Hello MSAK!")
    message_id = future.result()
```

### Transport Factory Usage
```python
from google.cloud.pubsublite.transport import TransportFactory, TransportType

# Create MSAK publisher transport directly
transport = TransportFactory.create_publisher_transport(
    transport_type=TransportType.MANAGED_KAFKA,
    topic="my-topic",
    msak_config=msak_config,
)
```