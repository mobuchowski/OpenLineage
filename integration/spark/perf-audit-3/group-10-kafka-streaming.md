# Performance Audit - Group 10: Kafka and Streaming Visitors

## Summary

None of the Kafka or streaming visitor classes make live network calls to broker metadata APIs, admin APIs, or schema registries. All metadata extraction is done by reading in-memory configuration fields (already populated by Spark before lineage collection begins) via reflection. The dominant performance concern in this group is the repeated use of uncached reflective field and method access, particularly per-event reflection in `KafkaRelationVisitor.apply()` and per-topic reflection in `KafkaMicroBatchStreamStrategy`, as well as one dead-code allocation in `KinesisMicroBatchStreamStrategy`.

## Classes Audited

- `KafkaRelationVisitor`: Extracts Kafka topic(s) and bootstrap servers from a `KafkaRelation`'s `sourceOptions` field via raw reflection.
- `KafkaMicroBatchStreamStrategy`: Reads bootstrap servers from `executorKafkaParams` and topics from a `KafkaSourceOffset` offset map, both via reflection.
- `KafkaBootstrapServerResolver`: Parses the bootstrap server config string into a `kafka://host:port` namespace URI — pure string/URI manipulation, no network.
- `StreamingDataSourceV2RelationVisitor`: Dispatcher that selects among Kafka, Kinesis, Mongo, and NoOp stream strategies based on class name.
- `StreamingDataSourceV2ScanRelationDatasetBuilder` (spark40): Spark 4.0-specific variant of the dispatcher; only handles Kafka and NoOp, missing Kinesis/Mongo strategies.
- `KinesisMicroBatchStreamStrategy`: Reads Kinesis stream name and endpoint URL from config fields via reflection; no AWS SDK calls.
- `MongoMicroBatchStreamStrategy`: Reads MongoDB connection URI, database, and collection from nested config map via reflection; no MongoDB admin calls.
- `StreamStrategy`: Abstract base class defining the contract; no logic.

## Performance Issues Found

### Uncached Reflective Field Access on Every `apply()` Call - Severity: MEDIUM

**Class**: `KafkaRelationVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/KafkaRelationVisitor.java:186-188`
**Description**: On every call to `apply()`, the code performs `relation.getClass().getDeclaredField("sourceOptions")` followed by `setAccessible(true)`. These two reflection operations are repeated from scratch every time a `KafkaRelation` logical plan node is visited. `getDeclaredField` involves a linear scan of the class's declared fields and cloning of the result. `setAccessible(true)` triggers an access check.
**Root Cause**: The `Field` object is a local variable inside `apply()`, so it is never reused across invocations. There is no static cache for the resolved `Field`.
**Code Evidence**:
```java
// KafkaRelationVisitor.java lines 186-188
Field sourceOptionsField = relation.getClass().getDeclaredField("sourceOptions");
sourceOptionsField.setAccessible(true);
sourceOptions = (Map<String, String>) sourceOptionsField.get(relation);
```
**Recommendation**: Cache the `Field` object in a `private static volatile Field SOURCE_OPTIONS_FIELD` (with lazy initialization guarded by a `null` check or `AtomicReference`). The class is already consistent about the target class (`KafkaRelation`), so the field descriptor is stable for the lifetime of the JVM.

---

### Reflective Method Invocation Per Topic Partition - Severity: LOW

**Class**: `TopicPartitionProxy` (used from `KafkaMicroBatchStreamStrategy`)
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/TopicPartitionProxy.java:73`
**Description**: For every `TopicPartition` in the offset map, `KafkaMicroBatchStreamStrategy.convertTopicPartitions()` constructs a `TopicPartitionProxy` and calls `proxy.topic()`, which in turn calls `MethodUtils.invokeMethod(topicPartition, "topic")`. `MethodUtils.invokeMethod` performs a method lookup (scanning accessible methods) on each call and does not cache the resolved `Method`. In a Kafka topic with many partitions (e.g., 100 partitions across 5 topics), this results in 100 reflective method lookups.
**Root Cause**: `MethodUtils.invokeMethod` is not cached; `TopicPartitionProxy` holds a direct reference to the underlying `TopicPartition` object (which is a concrete Kafka client class already on the classpath as shown by the import of `org.apache.kafka.common.TopicPartition` at line 21 of `KafkaMicroBatchStreamStrategy`). Since `TopicPartition` is already directly imported, the proxy's reflective indirection is unnecessary.
**Code Evidence**:
```java
// TopicPartitionProxy.java line 73
result = (T) MethodUtils.invokeMethod(topicPartition, methodName);

// KafkaMicroBatchStreamStrategy.java line 21 — direct import shows class is available
import org.apache.kafka.common.TopicPartition;
// line 64 — field is typed as TopicPartition directly
Optional<scala.collection.immutable.Map<TopicPartition, Long>> topicPartitionsMap =
    tryReadField(offset, "partitionToOffsets");
// line 108 — proxy created per partition
TopicPartitionProxy proxy = new TopicPartitionProxy(item);
```
**Recommendation**: Since `TopicPartition` is already directly on the classpath (the `import` at line 21 of `KafkaMicroBatchStreamStrategy` casts map keys directly to `TopicPartition`), `TopicPartitionProxy` is not providing any isolation. Replace the proxy call with a direct cast: `((TopicPartition) item).topic()`. This eliminates the per-partition reflective method lookup entirely.

---

### Dead-Code Object Allocation in Constructor - Severity: LOW

**Class**: `KinesisMicroBatchStreamStrategy`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/KinesisMicroBatchStreamStrategy.java:25`
**Description**: The constructor allocates a `new HostListNamespaceResolverConfig()` object whose result is immediately discarded — it is not assigned to any variable, stored in any field, or passed anywhere. This is dead-code garbage: an object is allocated and immediately eligible for GC.
**Root Cause**: Likely a leftover from a refactoring where some initialization or registration logic was moved out of the constructor but the `new` expression was not removed.
**Code Evidence**:
```java
// KinesisMicroBatchStreamStrategy.java line 25
new HostListNamespaceResolverConfig();
```
**Recommendation**: Remove this line entirely. It has no observable effect and just adds unnecessary heap allocation and GC pressure on every `KinesisMicroBatchStreamStrategy` construction.

---

### Reflective Field Access With Two-Level Traversal for Kinesis Config - Severity: LOW

**Class**: `KinesisMicroBatchStreamStrategy`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/KinesisMicroBatchStreamStrategy.java:34-40`
**Description**: The strategy reads `stream.options` and then `options.endpointUrl` and `options.streamName` through two levels of `FieldUtils.readField(..., true)` with `forceAccess=true`. Each call performs a field search up the class hierarchy (including superclasses) and forces accessibility. This is called on every micro-batch event for each Kinesis streaming query.
**Root Cause**: No caching of `Field` objects; `FieldUtils.readField` (commons-lang version, not commons-lang3) searches up the hierarchy each call. Also note that `KinesisMicroBatchStreamStrategy` imports `org.apache.commons.lang.reflect.FieldUtils` (the older commons-lang 2.x, deprecated) while the other strategies use `org.apache.commons.lang3.reflect.FieldUtils`, introducing an unnecessary dependency on a legacy library.
**Code Evidence**:
```java
// KinesisMicroBatchStreamStrategy.java lines 34-40
Optional<Object> options = tryReadField(stream, "options");
...
Optional<String> endpointUrl = tryReadField(options.get(), "endpointUrl");
Optional<String> streamName = tryReadField(options.get(), "streamName");
// tryReadField uses:
T value = (T) FieldUtils.readField(target, fieldName, true); // commons-lang 2.x
```
**Recommendation**: Cache the resolved `Field` objects as static finals and use direct field access after the first resolution. Also standardize to `commons-lang3` `FieldUtils` for consistency with the rest of the codebase.

## Clean Classes

- **`KafkaBootstrapServerResolver`**: Entirely clean. Takes an `Optional<String>` of already-loaded config, performs pure string manipulation (regex prefix addition, `URI.create`, host+port extraction), and returns a namespace string. No I/O, no network, no blocking operations. Zero performance concerns.

- **`StreamStrategy` (abstract base)**: Trivially clean — just stores four final fields in its constructor and declares an abstract method. No logic.

- **`NoOpStreamStrategy`**: Clean — returns an empty list immediately. This is the correct fallback for unrecognized stream types.

- **`StreamingDataSourceV2RelationVisitor`**: Clean at the dispatch level. The `isDefinedAt` check is a single `instanceof` test. The `selectStrategy` method does string equality comparisons on class names — lightweight. The actual work is delegated to strategy instances.

- **`StreamingDataSourceV2ScanRelationDatasetBuilder` (spark40)**: Clean dispatch logic. However, note it is missing `KinesisMicroBatchStreamStrategy` and `MongoMicroBatchStreamStrategy` support compared to the shared `StreamingDataSourceV2RelationVisitor`, meaning Kinesis and MongoDB streaming sources will silently fall through to `NoOpStreamStrategy` on Spark 4.0 — a correctness gap rather than a performance issue.

- **`KafkaMicroBatchStreamStrategy.getBootstrapServers()`**: Clean — reads `executorKafkaParams` (a plain `Map<String, Object>` already populated in the `KafkaMicroBatchStream` constructor by Spark, confirmed from Spark source at `KafkaMicroBatchStream.scala:58`) via one reflective field read, then does a simple map `.get("bootstrap.servers")`. No network, no broker connection.

- **`MongoMicroBatchStreamStrategy`**: Clean at a semantic level — reads pre-existing config fields from the stream object (two levels of reflection: `readConfig`, then `options` map), extracts `spark.mongodb.database`, `spark.mongodb.connection.uri`, and `spark.mongodb.collection` from the options map. No MongoDB admin calls, no `listCollections()`, no `admin` database access. The connection URI used to construct the namespace is from configuration, not from any live query to MongoDB.

## Spark/Iceberg Internals Investigated

- **`/home/bits/perf-audit/spark/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaMicroBatchStream.scala`** (lines 56-80): Confirmed that `executorKafkaParams` is a plain `ju.Map[String, Object]` constructor parameter stored as a private val in `KafkaMicroBatchStream`. It is a serialized copy of the Kafka client configuration intended for executor-side consumer creation. Reading it via reflection in `KafkaBootstrapServerResolver` does not trigger any Kafka broker connection — it is just a map lookup.
