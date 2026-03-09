# Performance Audit - Group 13: Snowflake and GCP/BigQuery Vendors

## Summary

The GCP vendor module (`GCPUtils`) is the primary performance concern: it issues multiple uncached HTTP calls to `http://metadata.google.internal/` on every single Spark listener event, with the number of calls ranging from 3 to 7 per event depending on the Dataproc resource type. The Snowflake classes are clean — they extract metadata purely from in-memory connection parameters without any JDBC connections, network calls, or external API calls. The BigQuery visitors are also clean: they derive table names from already-materialized relation objects or from local config parsing.

## Classes Audited

- `SnowflakeSaveIntoDataSourceCommandDatasetBuilder`: Extracts Snowflake output dataset from `SaveIntoDataSourceCommand` options map (pure string extraction).
- `SnowflakeRelationVisitor`: Extracts Snowflake input dataset from `SnowflakeRelation` parameters (pure string extraction, but uses `ClassLoader.loadClass` on every `isDefinedAt` call).
- `SnowflakeColumnLineageVisitor`: Delegates to `SnowflakeColumnLineageVisitorDelegate`; also calls `ClassLoader.loadClass` on every node visit in `isNotSnowflakeNode`.
- `SnowflakeColumnLineageVisitorDelegate`: Parses SQL queries through the local Rust-based `OpenLineageSql` parser; no Snowflake API calls.
- `BigQueryNodeInputVisitor`: Extracts BigQuery table name from an already-loaded `BigQueryRelation` object; uses reflection to handle connector version differences.
- `BigQueryNodeOutputVisitor`: Calls `BigQueryRelationProvider.createSparkBigQueryConfig()` to parse write config; uses reflection to extract table ID.
- `GcpJobFacetBuilder`: Calls `GCPUtils.getOriginFacetMap()` -> `createDataprocOriginMap()` on every Spark event.
- `GcpRunFacetBuilder`: Calls `GCPUtils.getDataprocRunFacetMap()` on every Spark event when running on Dataproc.

---

## Performance Issues Found

### Multiple Uncached HTTP Calls to GCP Metadata Service Per Event - Severity: HIGH

**Class**: `GCPUtils`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/vendor/gcp/src/main/java/io/openlineage/spark/agent/vendor/gcp/util/GCPUtils.java:240`

**Description**: Every call to `GcpRunFacetBuilder.build()` and `GcpJobFacetBuilder.build()` results in multiple synchronous blocking HTTP requests to `http://metadata.google.internal/computeMetadata/v1`. These calls are made on every `SparkListenerEvent`, which fires many times per job (job start, job end, stage start, stage end, SQL start, SQL end, etc.). There is no caching of any metadata results.

**Root Cause**: `fetchGCPMetadata()` performs a live HTTP GET to the GCP instance metadata server on every invocation. There are no static fields, `volatile` flags, or other memoization patterns in `GCPUtils`. Every call to `getDataprocRunFacetMap()` or `createDataprocOriginMap()` recomputes everything from scratch.

**HTTP Calls Per Event (worst case, BATCH resource type)**:

For `GcpRunFacetBuilder.build()` -> `getDataprocRunFacetMap()`:
1. `identifyResource()` -> `getDataprocBatchID()` -> HTTP GET `BATCH_ID_ENDPOINT`
2. Then in the BATCH case: `getDataprocBatchID()` -> HTTP GET `BATCH_ID_ENDPOINT` again (duplicate)
3. `getDataprocBatchUUID()` -> HTTP GET `BATCH_UUID_ENDPOINT`
4. `getGCPProjectId()` -> HTTP GET `PROJECT_ID_ENDPOINT`
= **4 HTTP calls** per event (with `BATCH_ID_ENDPOINT` hit twice due to `identifyResource` not returning early)

For `GcpJobFacetBuilder.build()` -> `createDataprocOriginMap()`:
1. `getDataprocRegion()` -> HTTP GET `DATAPROC_REGION_ENDPOINT`
2. `getGCPProjectId()` -> HTTP GET `PROJECT_ID_ENDPOINT`
3. `identifyResource()` -> `getDataprocBatchID()` -> HTTP GET `BATCH_ID_ENDPOINT`
4. Then in the BATCH arm: `getDataprocBatchID()` -> HTTP GET `BATCH_ID_ENDPOINT` again (duplicate)
= **4 HTTP calls** per event (with `BATCH_ID_ENDPOINT` hit twice)

For INTERACTIVE sessions, `SESSION_ID_ENDPOINT` is hit twice similarly.

Both builders fire on every `SparkListenerEvent`. Combined, a single job with 10 events generates ~80 HTTP calls to the metadata server.

**Code Evidence**:
```java
// GCPUtils.java:36
private static final String BASE_URI = "http://metadata.google.internal/computeMetadata/v1";

// GCPUtils.java:132-138 - identifyResource calls fetchGCPMetadata
private static ResourceType identifyResource(SparkContext context) {
    if ("yarn".equals(context.getConf().get(SPARK_MASTER, ""))) return ResourceType.CLUSTER;
    if (getDataprocBatchID(context).isPresent()) return ResourceType.BATCH;   // HTTP call #1
    if (getDataprocSessionID(context).isPresent()) return ResourceType.INTERACTIVE; // HTTP call #2
    return ResourceType.UNKNOWN;
}

// GCPUtils.java:88-118 - getDataprocRunFacetMap calls identifyResource AND then calls the same
// getDataprocBatchID/getDataprocSessionID again to populate the map
ResourceType resource = identifyResource(context);     // already fetched BATCH_ID
switch (resource) {
  case BATCH:
    getDataprocBatchID(context)...  // fetches BATCH_ID again - duplicate HTTP call
    getDataprocBatchUUID(context)... // new HTTP call
    break;
  case INTERACTIVE:
    getDataprocSessionID(context)... // fetches SESSION_ID again - duplicate HTTP call
    ...
}
getGCPProjectId(context)...  // another HTTP call

// GCPUtils.java:240-255 - each fetchGCPMetadata is a blocking HTTP call with no caching
private static Optional<String> fetchGCPMetadata(String httpEndpoint, SparkContext context) {
    String baseUri = context.getConf().get(GOOGLE_METADATA_API, BASE_URI);
    String httpURI = baseUri + httpEndpoint;
    HttpGet httpGet = new HttpGet(httpURI);
    httpGet.addHeader(METADATA_FLAVOUR, GOOGLE);
    try {
        return HTTP_CLIENT.execute(httpGet, response -> { ... });
    } catch (IOException e) {
        return Optional.empty();
    }
}
```

**Recommendation**: Cache all metadata values in static fields, populated lazily on first access. Since instance metadata does not change during a job's lifetime, a simple `static volatile String` per field (or a `ConcurrentHashMap<String, String>`) populated once via double-checked locking is sufficient. The `identifyResource()` result should also be cached as a `static volatile ResourceType`. Alternatively, populate all metadata values once at builder construction time (the `SparkContext` is available at constructor time) and store them as instance fields.

---

### Duplicate HTTP Calls Within a Single `identifyResource()` Call Chain - Severity: MEDIUM

**Class**: `GCPUtils`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/vendor/gcp/src/main/java/io/openlineage/spark/agent/vendor/gcp/util/GCPUtils.java:132`

**Description**: `identifyResource()` calls `getDataprocBatchID()` and `getDataprocSessionID()` to determine the resource type. Both `getDataprocRunFacetMap()` (line 90) and `createDataprocOriginMap()` (line 206) call `identifyResource()`, and then immediately call the same `getDataprocBatchID()` or `getDataprocSessionID()` again inside the `switch` block to fetch the value for the map. This means each of the two builder paths independently issues a duplicate HTTP request for the ID that was already fetched during resource type identification.

**Root Cause**: `identifyResource()` discards its fetched value (it only uses `isPresent()`) and the calling code must fetch again. The value is not threaded through.

**Code Evidence**:
```java
// getDataprocRunFacetMap, line 90
ResourceType resource = identifyResource(context);  // fetches BATCH_ID once

// line 101 - fetches BATCH_ID again for the actual value
getDataprocBatchID(context).ifPresent(p -> dataprocProperties.put("batchId", p));

// createDataprocOriginMap, line 206
switch (identifyResource(context)) {  // fetches BATCH_ID again
  case BATCH:
    resourceID = getDataprocBatchID(context).orElse("");  // fetches BATCH_ID yet again
```

**Recommendation**: Refactor `identifyResource()` to return an `Optional<String>` alongside the resource type (e.g., a small value object), or pass the already-fetched value into the switch handler. This eliminates at least one duplicate HTTP call per invocation path.

---

### `ClassLoader.loadClass` Called on Every `isDefinedAt` and `isNotSnowflakeNode` Invocation - Severity: LOW

**Class**: `SnowflakeRelationVisitor`, `SnowflakeColumnLineageVisitor`
**Location**:
- `/home/bits/perf-audit/OpenLineage/integration/spark/vendor/snowflake/src/main/java/io/openlineage/spark/agent/vendor/snowflake/lifecycle/SnowflakeRelationVisitor.java:44`
- `/home/bits/perf-audit/OpenLineage/integration/spark/vendor/snowflake/src/main/java/io/openlineage/spark/agent/vendor/snowflake/lifecycle/plan/column/SnowflakeColumnLineageVisitor.java:69`

**Description**: `SnowflakeRelationVisitor.isDefinedAt()` calls `isSnowflakeClass()` which invokes `Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_CLASS_NAME)` on every call. Similarly, `SnowflakeColumnLineageVisitor.isNotSnowflakeNode()` does the same. This visitor is called for every node in every logical plan tree traversal. While class loaders cache loaded classes internally after the first load, the overhead of `Thread.currentThread().getContextClassLoader()` + `loadClass()` for every node visit adds unnecessary CPU cost. The `SnowflakeColumnLineageVisitor` also emits `log.debug()` statements inside this hot path for every node visited, which can be expensive if debug logging is enabled.

**Root Cause**: The class object is not cached in a static field; it is re-looked-up via the class loader on each invocation.

**Code Evidence**:
```java
// SnowflakeRelationVisitor.java:43-46
protected boolean isSnowflakeClass(LogicalPlan plan) {
    try {
        Class c = Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_CLASS_NAME);
        return (plan instanceof LogicalRelation
            && c.isAssignableFrom(((LogicalRelation) plan).relation().getClass()));
    } catch (Exception e) { ... }
    return false;
}

// SnowflakeColumnLineageVisitor.java:67-76
log.debug("Checking if relation is Snowflake: {}", relation.getClass().getName());
try {
    Class<?> c = Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_CLASS_NAME);
    boolean isSnowflake = c.isAssignableFrom(relation.getClass());
    log.debug("Is Snowflake relation: {} (expected: {}, actual: {})",
        isSnowflake, SNOWFLAKE_CLASS_NAME, relation.getClass().getName());
```

**Recommendation**: Cache the loaded `Class<?>` in a static field, populated once on first successful load (or at class initialization time via a static initializer). Guard against `ClassNotFoundException` with a `static volatile boolean snowflakeClassAvailable` flag. Remove or guard the `log.debug()` calls with `log.isDebugEnabled()` checks in the hot path.

---

### `BigQueryNodeOutputVisitor` Calls `createSparkBigQueryConfig` On Every Write Event - Severity: LOW

**Class**: `BigQueryNodeOutputVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/BigQueryNodeOutputVisitor.java:63`

**Description**: In `apply()`, the code calls `bqRelationProvider.createSparkBigQueryConfig(sqlContext, saveCommand.options(), Option.apply(saveCommand.schema()))`. This creates a new `SparkBigQueryConfig` object on every call, which involves parsing all BigQuery connector options, merging defaults, and resolving the table reference. Depending on the connector implementation this may trigger a GCS or BQ metadata lookup to resolve default project/dataset values. In practice this is called once per write SQL operation (not on every event), so the impact is bounded but worth noting.

**Root Cause**: The `apply()` method is stateless and recreates the config object on every invocation. There is no result caching or short-circuiting for repeated calls to the same plan node.

**Code Evidence**:
```java
// BigQueryNodeOutputVisitor.java:57-66
private String getFromSaveIntoDataSourceCommand(
    SaveIntoDataSourceCommand saveCommand, SparkSession session) {
    SQLContext sqlContext = session.sqlContext();
    BigQueryRelationProvider bqRelationProvider =
        (BigQueryRelationProvider) saveCommand.dataSource();
    SparkBigQueryConfig config =
        bqRelationProvider.createSparkBigQueryConfig(   // config reconstruction on every call
            sqlContext, saveCommand.options(), Option.apply(saveCommand.schema()));
    return getBigQueryTableName(config).get();
}
```

**Recommendation**: This is inherently bounded (one call per write node, not per event), so the risk is low. However, if the connector's `createSparkBigQueryConfig` triggers remote calls, consider caching the resolved table name keyed on the `saveCommand`'s options hash or the command object identity.

---

## Clean Classes

- **`SnowflakeSaveIntoDataSourceCommandDatasetBuilder`**: Entirely clean. It extracts Snowflake connection properties (`sfurl`, `sfdatabase`, `sfschema`, `dbtable`, `query`) directly from the command's options map using simple `Map.get()` calls. No JDBC connections are opened, no Snowflake API calls are made, and no reflection is used. The schema is derived from the logical plan's in-memory output.

- **`SnowflakeDataset`**: Clean. Delegates to either a direct name construction (for `dbtable`) or the local Rust-based `OpenLineageSql.parse()` call (for `query`). The SQL parser is an in-process native library invocation with no network access.

- **`SnowflakeColumnLineageVisitorDelegate`**: Clean on the network side. It extracts Snowflake parameters from the already-loaded `SnowflakeRelation.params()` object (pure in-memory), parses any query SQL via the local `OpenLineageSql` parser, and maps column lineage through in-memory data structures. No Snowflake JDBC connections, no Snowflake REST API calls, and no credential refresh operations occur here.

- **`BigQueryNodeInputVisitor`**: Clean. For the standard `BigQueryRelation` path, the table name is retrieved via reflection (`tableName()` or `getTableName()`) from an already-loaded relation object — no BigQuery REST API call is made. For the `DirectBigQueryRelation` path with a custom query, it reads from the already-constructed `SparkBigQueryConfig` object via field reflection. The `FieldUtils.readField(relation, "options", true)` call uses Apache Commons reflection to access a private field, which has minor overhead but no network I/O.

- **`BigQueryNodeOutputVisitor`**: Mostly clean from a network-call perspective. It does not call the BigQuery REST API for schema or table metadata. The `extractDatasetIdentifierFromTableId` method uses reflection to call a static utility method, which is a CPU-only operation.

---

## Spark/Iceberg Internals Investigated

No Spark or Iceberg source files were consulted for this audit. All findings are based on the OpenLineage source files listed above and on static analysis of the call chains within `GCPUtils.java`. The Snowflake and BigQuery connector behaviors referenced (e.g., `BigQueryRelationProvider.createSparkBigQueryConfig`, `SnowflakeRelation.params()`) are inferred from the connector API usage patterns in the OpenLineage code.
