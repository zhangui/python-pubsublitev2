# Kafka AdminClient Implementation Plan

## Overview
Implement Kafka transport for AdminServiceClient to enable Pub/Sub Lite admin operations on Managed Service for Apache Kafka (MSAK).

## Architecture Analysis

### Current Architecture (Pub/Sub Lite)
```
AdminClient (public API)
    ↓
AdminClientImpl (internal implementation)
    ↓
AdminServiceClient (gRPC client)
    ↓
AdminServiceTransport (grpc / grpc_asyncio)
```

### Existing Kafka Transports
- **PublisherServiceKafkaTransport**: Uses AsyncKafkaPublisher, handles streaming publish requests
- **SubscriberServiceKafkaTransport**: Uses AsyncKafkaSubscriber, handles bidirectional streaming

### Target Architecture (Kafka)
```
AdminClient (public API)
    ↓
AdminClientImpl (internal implementation)
    ↓
AdminServiceClient (multi-transport client)
    ↓
AdminServiceKafkaTransport (NEW - uses confluent_kafka.admin.AdminClient)
```

## Kafka Admin API Capabilities

The `confluent_kafka.admin.AdminClient` provides:

**Topic Operations:**
- `create_topics()` - Create new topics
- `delete_topics()` - Delete topics
- `describe_topics()` - Get topic metadata (partitions, replicas, config)
- `list_topics()` - List all topics
- `create_partitions()` - Increase partition count

**Configuration Operations:**
- `describe_configs()` - Get resource configurations
- `alter_configs()` - Update configurations (deprecated)
- `incremental_alter_configs()` - Update configurations incrementally

**Consumer Group Operations:**
- `list_consumer_groups()` - List consumer groups
- `describe_consumer_groups()` - Get consumer group details
- `delete_consumer_groups()` - Delete consumer groups
- `list_consumer_group_offsets()` - Get offsets for consumer group
- `alter_consumer_group_offsets()` - Set offsets for consumer group
- `delete_records()` - Delete records up to offset

**ACL Operations:**
- `create_acls()`, `delete_acls()`, `describe_acls()`

**Other:**
- `describe_cluster()` - Get cluster metadata
- `elect_leaders()` - Trigger leader election

## Pub/Sub Lite Admin Operations Mapping

### ✅ Directly Mappable to Kafka

| Pub/Sub Lite Operation | Kafka Equivalent | Implementation Difficulty |
|------------------------|------------------|---------------------------|
| `create_topic()` | `create_topics()` | **EASY** - Direct mapping |
| `delete_topic()` | `delete_topics()` | **EASY** - Direct mapping |
| `get_topic()` | `describe_topics()` | **MEDIUM** - Need to convert metadata |
| `list_topics()` | `list_topics()` | **EASY** - Direct mapping with filtering |
| `update_topic()` | `incremental_alter_configs()` | **MEDIUM** - Need to map Pub/Sub configs to Kafka configs |
| `get_topic_partition_count()` | `describe_topics()` → partition_count | **EASY** - Extract from metadata |

### ⚠️ Subscription Operations (Consumer Groups)

| Pub/Sub Lite Operation | Kafka Equivalent | Notes |
|------------------------|------------------|-------|
| `create_subscription()` | Create consumer group (implicit) | Consumer groups are created when first consumer joins |
| `delete_subscription()` | `delete_consumer_groups()` | **MEDIUM** - Direct mapping |
| `get_subscription()` | `describe_consumer_groups()` | **HARD** - Need to synthesize Subscription proto from consumer group |
| `list_subscriptions()` | `list_consumer_groups()` | **HARD** - Need to synthesize multiple Subscription protos |
| `update_subscription()` | Update consumer group config | **HARD** - Limited support in Kafka |
| `seek_subscription()` | `alter_consumer_group_offsets()` | **MEDIUM** - Map BacklogLocation/PublishTime to offsets |
| `list_topic_subscriptions()` | Filter consumer groups by topic | **HARD** - Need to query all groups and filter |

### ❌ Not Supported in Kafka

| Pub/Sub Lite Operation | Reason |
|------------------------|--------|
| `create_reservation()` | Reservations are Pub/Sub Lite concept (capacity reservations) |
| `delete_reservation()` | Not applicable to Kafka |
| `get_reservation()` | Not applicable to Kafka |
| `list_reservations()` | Not applicable to Kafka |
| `update_reservation()` | Not applicable to Kafka |
| `list_reservation_topics()` | Not applicable to Kafka |

## Implementation Plan

### Phase 1: Core Infrastructure (REQUIRED)

#### 1.1 Create AdminServiceKafkaTransport
**File**: `google/cloud/pubsublite_v1/services/admin_service/transports/kafka.py`

```python
class AdminServiceKafkaTransport(AdminServiceTransport):
    """Kafka transport for AdminService."""

    def __init__(
        self,
        *,
        host: str = "pubsublite.googleapis.com",
        credentials=None,
        admin_config: Dict[str, Any] = None,  # Kafka admin config
        **kwargs
    ):
        # Store admin config
        self._admin_config = admin_config
        self._ignore_credentials = True

        # Create Kafka AdminClient
        from confluent_kafka.admin import AdminClient
        self._admin_client = AdminClient(admin_config)

        # Call base __init__
        super().__init__(...)

    # Implement all required methods...
```

**Pattern to follow**: Similar to PublisherServiceKafkaTransport and SubscriberServiceKafkaTransport

#### 1.2 Register Kafka Transport
**File**: `google/cloud/pubsublite_v1/services/admin_service/transports/__init__.py`

```python
from .kafka import AdminServiceKafkaTransport

_transport_registry["kafka"] = AdminServiceKafkaTransport
```

**File**: `google/cloud/pubsublite_v1/services/admin_service/client.py`

Update `_transport_registry` to include Kafka transport.

#### 1.3 Update AdminClient Constructor
**File**: `google/cloud/pubsublite/admin_client.py`

```python
def __init__(
    self,
    region: CloudRegion,
    credentials: Optional[Credentials] = None,
    transport: Optional[str] = None,
    client_options: Optional[ClientOptions] = None,
    admin_config: Optional[Dict[str, Any]] = None,  # NEW - Kafka admin config
):
    if transport == "kafka":
        # Pass admin_config to AdminServiceClient
        self._impl = AdminClientImpl(
            AdminServiceClient(
                transport=transport,
                admin_config=admin_config,
            ),
            region,
        )
    else:
        # Existing Pub/Sub Lite logic
        ...
```

### Phase 2: Topic Operations (HIGH PRIORITY - EASY)

Implement in `AdminServiceKafkaTransport`:

#### 2.1 create_topic()
```python
@property
def create_topic(self) -> Callable:
    def _create_topic(request):
        topic_name = request.topic_id
        num_partitions = request.topic.partition_config.count
        replication_factor = request.topic.partition_config.capacity.publish_mib_per_sec  # Map appropriately

        from confluent_kafka.admin import NewTopic
        new_topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config={...}  # Map Pub/Sub Lite configs to Kafka configs
        )

        fs = self._admin_client.create_topics([new_topic])
        # Wait for future
        fs[topic_name].result()

        # Return Topic proto
        return admin.Topic(name=f"projects/.../topics/{topic_name}", ...)

    return _create_topic
```

#### 2.2 delete_topic()
```python
@property
def delete_topic(self) -> Callable:
    def _delete_topic(request):
        topic_name = extract_topic_name(request.name)
        fs = self._admin_client.delete_topics([topic_name])
        fs[topic_name].result()

    return _delete_topic
```

#### 2.3 get_topic() & get_topic_partitions()
```python
@property
def get_topic(self) -> Callable:
    def _get_topic(request):
        topic_name = extract_topic_name(request.name)

        # Use describe_topics
        from confluent_kafka.admin import TopicMetadata
        metadata = self._admin_client.list_topics(topic=topic_name)

        # Convert Kafka topic metadata to Pub/Sub Lite Topic proto
        return admin.Topic(
            name=request.name,
            partition_config=admin.Topic.PartitionConfig(
                count=len(metadata.topics[topic_name].partitions)
            ),
            # Map other fields...
        )

    return _get_topic
```

#### 2.4 list_topics()
```python
@property
def list_topics(self) -> Callable:
    def _list_topics(request):
        metadata = self._admin_client.list_topics()

        # Filter and convert to Topic protos
        topics = []
        for topic_name, topic_meta in metadata.topics.items():
            if not topic_name.startswith('_'):  # Filter internal topics
                topics.append(admin.Topic(
                    name=f"{request.parent}/topics/{topic_name}",
                    partition_config=admin.Topic.PartitionConfig(
                        count=len(topic_meta.partitions)
                    ),
                ))

        return admin.ListTopicsResponse(topics=topics)

    return _list_topics
```

#### 2.5 update_topic()
```python
@property
def update_topic(self) -> Callable:
    def _update_topic(request):
        topic_name = extract_topic_name(request.topic.name)

        # Map update_mask fields to Kafka config updates
        from confluent_kafka.admin import ConfigResource, ResourceType

        config_updates = {}
        for path in request.update_mask.paths:
            if path == "partition_config.count":
                # Use create_partitions()
                pass
            elif path.startswith("retention_config"):
                # Map to retention.ms
                config_updates["retention.ms"] = ...
            # ... map other fields

        if config_updates:
            resource = ConfigResource(ResourceType.TOPIC, topic_name)
            for key, value in config_updates.items():
                resource.set_config(key, value)

            fs = self._admin_client.incremental_alter_configs([resource])
            fs[resource].result()

        return request.topic

    return _update_topic
```

### Phase 3: Subscription Operations (MEDIUM PRIORITY - COMPLEX)

#### 3.1 Map Subscription to Consumer Group Concept

**Key Challenge**: Pub/Sub Lite Subscription != Kafka Consumer Group
- Subscriptions have delivery configs, export configs, etc.
- Consumer groups are simpler - just group ID and offsets

**Solution**: Store subscription metadata in Kafka topic configs or separate metadata topic

#### 3.2 create_subscription()
```python
@property
def create_subscription(self) -> Callable:
    def _create_subscription(request):
        # Consumer groups are created implicitly, but we can:
        # 1. Validate topic exists
        # 2. Initialize offsets based on skip_backlog
        # 3. Store subscription metadata somewhere (Kafka topic config?)

        subscription_id = request.subscription_id
        topic_name = extract_topic_name(request.subscription.topic)

        # Validate topic exists
        self._admin_client.list_topics(topic=topic_name)

        # If skip_backlog=False, set offsets to earliest
        # If skip_backlog=True, set offsets to latest
        # Use alter_consumer_group_offsets() when first consumer joins

        return admin.Subscription(
            name=f"{request.parent}/subscriptions/{subscription_id}",
            topic=request.subscription.topic,
            # Populate other fields
        )

    return _create_subscription
```

#### 3.3 delete_subscription()
```python
@property
def delete_subscription(self) -> Callable:
    def _delete_subscription(request):
        group_id = extract_subscription_name(request.name)
        fs = self._admin_client.delete_consumer_groups([group_id])
        fs[group_id].result()

    return _delete_subscription
```

#### 3.4 seek_subscription()
```python
@property
def seek_subscription(self) -> Callable:
    def _seek_subscription(request):
        group_id = extract_subscription_name(request.name)

        # Get topic partitions
        topic_name = ...  # Need to look up or store mapping
        metadata = self._admin_client.list_topics(topic=topic_name)

        # Create TopicPartition objects with new offsets
        from confluent_kafka.admin import TopicPartition

        partitions = []
        for partition_id in metadata.topics[topic_name].partitions:
            if request.HasField("time_target"):
                # Map timestamp to offset using list_offsets()
                offset = self._timestamp_to_offset(topic_name, partition_id, request.time_target)
            elif request.named_target == SeekSubscriptionRequest.NamedTarget.HEAD:
                offset = OFFSET_END
            else:  # TAIL
                offset = OFFSET_BEGINNING

            partitions.append(TopicPartition(topic_name, partition_id, offset))

        # Alter offsets
        from confluent_kafka.admin import ConsumerGroupTopicPartitions
        group_tp = ConsumerGroupTopicPartitions(group_id, partitions)
        fs = self._admin_client.alter_consumer_group_offsets([group_tp])
        fs[group_id].result()

        # Return Operation proto
        return Operation(name=f"operations/{uuid4()}", done=True)

    return _seek_subscription
```

### Phase 4: Reservation Operations (LOW PRIORITY - NOT SUPPORTED)

**Recommendation**: Raise `NotImplementedError` for all reservation operations when using Kafka transport.

```python
@property
def create_reservation(self) -> Callable:
    def _not_supported(request):
        raise NotImplementedError(
            "Reservation operations are not supported with Kafka transport. "
            "Reservations are a Pub/Sub Lite capacity management feature."
        )
    return _not_supported
```

Apply same pattern for:
- `delete_reservation()`
- `get_reservation()`
- `list_reservations()`
- `update_reservation()`
- `list_reservation_topics()`

### Phase 5: Configuration Mapping

#### 5.1 Pub/Sub Lite → Kafka Config Mapping

| Pub/Sub Lite Config | Kafka Config | Notes |
|---------------------|--------------|-------|
| `retention_config.period` | `retention.ms` | Convert duration to milliseconds |
| `retention_config.per_partition_bytes` | `retention.bytes` | Direct mapping |
| `partition_config.count` | `num.partitions` | Set on creation / use create_partitions() |
| `partition_config.capacity.publish_mib_per_sec` | `replication.factor` | Approximate mapping |
| `partition_config.capacity.subscribe_mib_per_sec` | N/A | No direct equivalent |

#### 5.2 Kafka → Pub/Sub Lite Config Mapping

Reverse mapping for `get_topic()` and `list_topics()`.

### Phase 6: Testing

#### 6.1 Unit Tests
**File**: `tests/unit/gapic/pubsublite_v1/test_admin_service_kafka.py`

```python
def test_create_topic_kafka():
    transport = AdminServiceKafkaTransport(admin_config={...})
    # Mock confluent_kafka.admin.AdminClient
    # Test create_topic() calls
    ...

def test_delete_topic_kafka():
    ...

def test_list_topics_kafka():
    ...
```

#### 6.2 Integration Tests
**File**: `tests/integration/admin_kafka_test.py`

```python
def test_admin_client_kafka_create_delete_topic():
    admin = AdminClient(
        region=CloudRegion("us-central1"),
        transport="kafka",
        admin_config={
            'bootstrap.servers': 'localhost:9092'
        }
    )

    # Test topic lifecycle
    topic = admin.create_topic(Topic(...))
    topics = admin.list_topics(location_path)
    admin.delete_topic(topic_path)
```

#### 6.3 Sample Scripts
**File**: `samples/msak/admin_operations.py`

```python
#!/usr/bin/env python
"""Admin operations on MSAK using AdminClient with Kafka transport."""

from google.cloud.pubsublite import AdminClient
from google.cloud.pubsublite.types import CloudRegion, TopicPath, LocationPath
from google.cloud.pubsublite_v1 import Topic

# Try to use TokenProvider for OAuth
try:
    from tokenprovider import TokenProvider
    token_provider = TokenProvider()
    admin_config = {
        'bootstrap.servers': 'bootstrap.example.managedkafka.goog:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'oauth_cb': token_provider.get_token,
    }
except ImportError:
    admin_config = {'bootstrap.servers': 'localhost:9092'}

# Create admin client with Kafka transport
admin = AdminClient(
    region=CloudRegion("us-central1"),
    transport="kafka",
    admin_config=admin_config
)

# Create topic
topic = admin.create_topic(
    Topic(
        name="projects/my-project/locations/us-central1/topics/my-kafka-topic",
        partition_config=Topic.PartitionConfig(count=3),
    )
)
print(f"Created topic: {topic.name}")

# List topics
location = LocationPath("my-project", "us-central1")
topics = admin.list_topics(location)
print(f"Topics: {[t.name for t in topics]}")

# Delete topic
topic_path = TopicPath("my-project", "us-central1", "my-kafka-topic")
admin.delete_topic(topic_path)
print(f"Deleted topic: {topic_path}")
```

## Implementation Complexity Estimates

| Component | Lines of Code | Difficulty | Time Estimate |
|-----------|---------------|------------|---------------|
| AdminServiceKafkaTransport (base) | 150 | Medium | 2 hours |
| Topic operations (create/delete/get/list) | 300 | Easy-Medium | 4 hours |
| Topic update operation | 100 | Medium | 2 hours |
| Subscription create/delete | 200 | Medium | 3 hours |
| Subscription seek | 150 | Hard | 4 hours |
| Subscription get/list | 200 | Hard | 4 hours |
| Config mapping utilities | 150 | Medium | 2 hours |
| Transport registration | 50 | Easy | 1 hour |
| AdminClient updates | 100 | Easy | 1 hour |
| Unit tests | 500 | Medium | 6 hours |
| Integration tests | 300 | Medium | 4 hours |
| Documentation & samples | 200 | Easy | 2 hours |
| **TOTAL** | **~2,400** | **-** | **35 hours** |

## Recommended Phased Rollout

### MVP (Minimum Viable Product) - 8 hours
- AdminServiceKafkaTransport infrastructure
- Topic operations: create, delete, get, list
- Basic unit tests
- Simple sample script

### V1 (Production Ready) - 16 hours
- All topic operations including update
- Subscription create, delete
- Integration tests
- Documentation

### V2 (Full Feature) - 35 hours
- Subscription get, list, seek
- Consumer group offset management
- Comprehensive testing
- Production samples with OAuth

## Open Questions & Decisions

1. **Subscription Metadata Storage**: Where to store Pub/Sub Lite subscription metadata (delivery config, export config)?
   - **Option A**: Kafka topic configs (limited space)
   - **Option B**: Separate metadata topic (adds complexity)
   - **Option C**: In-memory only (lose metadata on restart)
   - **Recommendation**: Option C for MVP, Option B for production

2. **Consumer Group Naming**: How to map subscription names to consumer group IDs?
   - **Current**: `pubsublite-{topic}-p{partition}` for subscribers
   - **Recommendation**: Use subscription name directly as group ID

3. **Partition Assignment**: How to handle partition assignment for subscriptions?
   - Kafka uses consumer group rebalancing
   - Pub/Sub Lite has explicit partition assignment
   - **Recommendation**: Document that Kafka transport uses standard Kafka partition assignment

4. **Error Handling**: How to map Kafka errors to Pub/Sub Lite errors?
   - Map `TopicAlreadyExistsException` → `ALREADY_EXISTS`
   - Map `UnknownTopicOrPartitionException` → `NOT_FOUND`
   - etc.

5. **Seek Timestamp Mapping**: How to efficiently map PublishTime/EventTime to Kafka offsets?
   - Use `list_offsets()` with timestamp
   - May need binary search for exact match

## Dependencies

**Required**:
- `confluent-kafka >= 2.0.0` (already a dependency)

**Optional for samples**:
- `tokenprovider` module for OAuth authentication

## Documentation Updates

1. **README**: Add section on Kafka admin operations
2. **KAFKA_TRANSPORT.md**: New doc explaining Kafka admin transport
3. **API docs**: Update AdminClient docstring to mention Kafka transport
4. **Migration guide**: Guide for users switching from Pub/Sub Lite to Kafka

## Success Criteria

✅ AdminClient can create/delete/list topics on MSAK
✅ AdminClient can create/delete subscriptions (consumer groups)
✅ All operations work with OAuth authentication
✅ 95% code coverage for Kafka transport
✅ Integration tests pass against real MSAK cluster
✅ Sample scripts work end-to-end
✅ Documentation is complete and accurate

## Non-Goals (Out of Scope)

- ACL management (Kafka-specific)
- Cluster operations (elect_leaders, describe_cluster)
- SCRAM credential management
- Full parity with all Pub/Sub Lite subscription features
- Support for Kafka topics created outside Pub/Sub Lite API
