# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
- Run all unit tests: `nox -s unit`
- Run specific test: `nox -s unit-3.12 -- -k <test_name>`
- Run system tests: `nox -s system`
- Run coverage: `nox -s cover` (requires 96% coverage)

### Linting and Formatting
- Run linting: `nox -s lint`
- Format code: `nox -s blacken`
- Format with imports: `nox -s format` (runs isort + black)
- Check setup.py: `nox -s lint_setup_py`
- Type checking: `nox -s pytype`

### Documentation
- Build docs: `nox -s docs`
- Build docfx: `nox -s docfx`

## Code Architecture

This is the **Google Cloud Pub/Sub Lite Python client library**, which provides both low-level gRPC clients and high-level Cloud Pub/Sub-compatible clients.

### Key Architecture Components

**Two-Layer Structure:**
1. **Low-level gRPC clients** (`google.cloud.pubsublite_v1`): Generated GAPIC clients for direct service interaction
2. **High-level clients** (`google.cloud.pubsublite.cloudpubsub`): Cloud Pub/Sub-compatible abstraction layer

**Core Modules:**
- `google.cloud.pubsublite_v1.services`: Generated gRPC service clients (AdminService, PublisherService, SubscriberService, etc.)
- `google.cloud.pubsublite.cloudpubsub`: High-level publisher/subscriber clients with Cloud Pub/Sub compatibility
- `google.cloud.pubsublite.internal`: Internal utilities, wire protocols, partition assignment
- `google.cloud.pubsublite.types`: Type definitions and data structures

**Publisher Architecture:**
- `MultiplexedPublisherClient`: Manages multiple topic publishers
- `SinglePublisher`: Handles publishing to a single topic partition
- `ClientMultiplexer`: Manages client lifecycle and routing

**Subscriber Architecture:**  
- `MultiplexedSubscriberClient`: Manages multiple subscription consumers
- `AssigningSubscriber`: Handles partition assignment for subscriptions
- `SinglePartitionSubscriber`: Consumes from a single partition

**Key Design Patterns:**
- Multiplexing pattern for managing multiple publishers/subscribers
- Factory pattern for creating clients
- Interface-based design with implementations
- Async and sync variants for most operations

### Kafka Backend Integration (New)

**Multi-Backend Publisher:**
- `PublisherClient` now supports both Pub/Sub Lite and Kafka backends
- `AsyncKafkaPublisher`: Internal Kafka implementation using confluent-kafka library
- `KafkaConfig`: Configuration for Kafka connection and authentication

**Backend Selection:**
- Environment variable: `PUBSUBLITE_USE_KAFKA=true/false`
- Constructor parameters: `use_kafka=True/False` + `kafka_bootstrap_servers`
- Default: Pub/Sub Lite (backward compatibility)

**Kafka Integration:**
- Uses OAuth/SASL_SSL authentication with Google Cloud credentials or token providers
- Converts TopicPath to Kafka topic names automatically
- Maps Pub/Sub message format to Kafka (data→value, ordering_key→partition_key, attrs→headers)
- Returns topic:partition:offset as ack ID
- Reuses all existing multiplexing and error handling infrastructure

## Dependencies

### Required
- Standard Pub/Sub Lite dependencies (always installed)

### Optional
- `confluent-kafka >= 2.0.0`: For Kafka backend support
- Install with: `pip install google-cloud-pubsublite[kafka]`

## Development Notes

- Python versions: 3.8+ (defined in `noxfile.py`)
- Uses `nox` for testing automation instead of tox
- Pre-commit hooks available with `pre-commit install`
- Generated code in `**/gapic/**`, `**/services/**`, `**/types/**` should not be manually edited
- Uses `overrides` decorator for interface implementations
- Kafka functionality is optional and gracefully degrades if confluent-kafka not installed