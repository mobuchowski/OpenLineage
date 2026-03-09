# Performance Audit - Group 4: Iceberg Catalog Handlers

## Summary

The Iceberg catalog handler group has two significant architectural performance issues. First, `IcebergHandler.getIcebergTable()` calls `sparkCatalog.loadTable()` on every invocation, which triggers an HTTP request to the REST catalog on each call — and this method is called twice per lineage event (once for `getDatasetIdentifier`, once for `getDatasetVersion`). Second, the entire handler list, including `IcebergHandler` itself, is re-instantiated and `IcebergHandler.class.getClassLoader().loadClass()` is called on every single `CatalogUtils3` method invocation, meaning each event triggers multiple class-loading probes. The `GlueCatalogTypeHandler` can also trigger an EC2 IMDS HTTP call and an AWS STS API call on the first event where `AWS_DEFAULT_REGION` and an explicit catalog ID are not set.

## Classes Audited

- `IcebergHandler`: Main Iceberg catalog handler; resolves dataset identifier, version, storage, and catalog facets
- `BaseCatalogTypeHandler`: Abstract base for all catalog-type-specific handlers; provides `defaultTableLocation` and `catalogProperties`
- `HiveCatalogTypeHandler`: Resolves identifier for Hive-backed Iceberg catalogs; reads URI from config or Spark context
- `GlueCatalogTypeHandler`: Resolves identifier for Glue-backed Iceberg catalogs; calls `AwsUtils.getGlueArn()` which can make HTTP calls
- `HadoopCatalogTypeHandler`: Resolves identifier for Hadoop file-system catalogs; reads warehouse from config only
- `RestCatalogTypeHandler`: Resolves identifier for REST-backed Iceberg catalogs; reads URI from config only
- `NessieCatalogTypeHandler`: Resolves identifier for Nessie catalogs; reads URI from config only
- `BigQueryMetastoreCatalogTypeHandler`: Resolves identifier for BigQuery Metastore catalogs; reads warehouse from config
- `IcebergCommitReportOutputDatasetFacetBuilder`: Reads pre-captured commit metrics from in-memory holder; no I/O
- `IcebergScanReportInputDatasetFacetBuilder`: Uses reflection to call `table()` then accesses `currentSnapshot()`; reads from pre-captured scan metrics
- `IcebergInputStatisticsInputDatasetFacetBuilder`: Uses reflection to call `tasks()` on a scan object; tasks are lazily cached by Iceberg
- `CommitReportsFacetBuilder`: Pure data transformation from `CommitReport` to facet; no I/O
- `ScanReportsFacetBuilder`: Pure data transformation from `ScanReport` to facet; no I/O

---

## Performance Issues Found

### [Double loadTable() Per Event] - Severity: HIGH

**Class**: `IcebergHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/iceberg/IcebergHandler.java:154` and `:244`

**Description**: `getIcebergTable()` is called in both `getDatasetIdentifier()` (line 154) and `getDatasetVersion()` (line 244). Both are called for every lineage event involving an Iceberg table. Each call to `getIcebergTable()` invokes `sparkCatalog.loadTable(identifier)` (line 254), which is a round-trip to the catalog backend. There is no local caching of the loaded `Table` object within a single event processing cycle.

**Root Cause**: The `getIcebergTable` helper is private and stateless; there is no mechanism to share the loaded `Table` instance between the two public methods that both need it in the same event.

**Code Evidence**:
```java
// getDatasetIdentifier() - line 154:
Optional<Table> table = getIcebergTable(tableCatalog, identifier);

// getDatasetVersion() - line 244:
return getIcebergTable(tableCatalog, identifier)
    .map(table -> table.currentSnapshot())
    .map(snapshot -> Long.toString(snapshot.snapshotId()));

// getIcebergTable() - line 254:
SparkTable sparkTable = (SparkTable) sparkCatalog.loadTable(identifier);
```

For REST catalogs, `sparkCatalog.loadTable()` delegates through `CachingCatalog` (if enabled) or directly to `RESTSessionCatalog.loadTable()`, which issues an HTTP GET to the REST catalog endpoint (`/v1/namespaces/{ns}/tables/{table}`). Even with ETag-based conditional requests, a network round-trip still occurs unless the server returns HTTP 304.

**Recommendation**: Cache the `Optional<Table>` result within the duration of a single event. A simple approach is to add a method `getDatasetIdentifierAndVersion()` that loads the table once and returns both values, or to accept the `Table` object as a parameter to both methods. Alternatively, expose a request-scoped cache (e.g., a `ThreadLocal<Map<Identifier, Table>>`) keyed on `(tableCatalog, identifier)` and populated on first call within an event.

---

### [Handler List and Class Loading Repeated Per Call] - Severity: MEDIUM

**Class**: `CatalogUtils3` (calling `IcebergHandler.hasClasses()`)
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/CatalogUtils3.java:26-36` and `IcebergHandler.java:61-68`

**Description**: `CatalogUtils3.getHandlers(context)` constructs a fresh `List<CatalogHandler>` on every invocation, instantiating six handler objects and calling `hasClasses()` on each. `IcebergHandler.hasClasses()` calls `IcebergHandler.class.getClassLoader().loadClass("org.apache.iceberg.catalog.Catalog")` on every call. For a single Iceberg event, `getHandlers()` is called by `getDatasetIdentifier()`, `getCatalogHandler()` (called twice inside `addStorageAndCatalogFacets()` via `getStorageDatasetFacet()` and `getCatalogDatasetFacet()`), and `getDatasetVersion()` — meaning four separate handler-list builds per event, each with a `loadClass` probe.

**Root Cause**: `getHandlers()` is a static method that creates a new list every time rather than caching it. `hasClasses()` does not cache its result — it calls `loadClass` unconditionally on every invocation.

**Code Evidence**:
```java
// CatalogUtils3.java:26-35
private static List<CatalogHandler> getHandlers(OpenLineageContext context) {
    List<CatalogHandler> handlers = Arrays.asList(
        new IcebergHandler(context), ...);
    return handlers.stream().filter(CatalogHandler::hasClasses).collect(Collectors.toList());
}

// IcebergHandler.java:61-68
public boolean hasClasses() {
    try {
        IcebergHandler.class.getClassLoader().loadClass("org.apache.iceberg.catalog.Catalog");
        return true;
    } catch (Exception e) { ... }
    return false;
}
```

`loadClass("org.apache.iceberg.catalog.Catalog")` will return the cached class after the first load (JVM class loader caches loaded classes), so the overhead is a hash-table lookup in the classloader — cheap but not free. The bigger cost is the repeated object allocation of six handler instances and the stream filtering on every event.

**Recommendation**: Cache the filtered handler list in a `static` field on `CatalogUtils3`, initialized once per `OpenLineageContext` (or per classloader). Alternatively, cache the `hasClasses()` result as a static final boolean computed at class-initialization time (`static final boolean HAS_CLASSES = probe();`) so the classloader lookup runs exactly once.

---

### [EC2 IMDS HTTP Call in GlueCatalogTypeHandler] - Severity: HIGH

**Class**: `GlueCatalogTypeHandler` → `AwsUtils` → `AwsUtils.awsRegion()` → `AwsUtils.getRegionFromEc2Metadata()`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/AwsUtils.java:121-127` and `133-186`

**Description**: When `GlueCatalogTypeHandler.getIdentifier()` is called, it delegates to `AwsUtils.getGlueArn()`, which calls `awsRegion()`. If neither `AWS_DEFAULT_REGION` nor `AWS_REGION` environment variables are set, the code falls back to `getRegionFromEc2Metadata()`, which opens an HTTP connection to the EC2 instance metadata service (`http://169.254.169.254/latest/api/token` and `http://169.254.169.254/latest/meta-data/placement/region`) with 2-second connect and read timeouts each. If the host is not an EC2 instance (e.g., a local dev machine, a non-AWS cloud) this will block for up to 4 seconds on every event for every Glue-backed Iceberg table.

**Root Cause**: There is no caching of the region lookup result in `AwsUtils.awsRegion()`. `getRegionFromEc2Metadata()` is called fresh on every event when the environment variable path fails.

**Code Evidence**:
```java
// AwsUtils.java:108-127
private static Optional<String> awsRegion() {
    Optional<String> envRegion = Optional.ofNullable(System.getenv("AWS_DEFAULT_REGION"))...;
    if (envRegion.isPresent()) { return envRegion; }
    // Fallback: try EC2 instance metadata service
    try {
        return getRegionFromEc2Metadata();  // opens HTTP connections with 2s timeouts
    } catch (Exception e) { ... }
    return Optional.empty();
}

// getRegionFromEc2Metadata() - line 137-138:
String tokenUrl = "http://169.254.169.254/latest/api/token";
tokenConnection.setConnectTimeout(2000);
tokenConnection.setReadTimeout(2000);
```

Note: `AwsAccountIdFetcher.getAccountId()` (used when no explicit catalog ID is set) does cache its STS result in a static field. However, the region lookup does not.

**Recommendation**: Cache the result of `awsRegion()` in a static field in `AwsUtils`, similar to how `AwsAccountIdFetcher` caches the account ID. For example:
```java
private static volatile Optional<String> cachedRegion = null;
private static Optional<String> awsRegion() {
    if (cachedRegion == null) { cachedRegion = computeRegion(); }
    return cachedRegion;
}
```

---

### [Spark conf.getAll() Called Twice Per Event in IcebergHandler] - Severity: LOW

**Class**: `IcebergHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/iceberg/IcebergHandler.java:83` and `:130`

**Description**: `session.conf().getAll()` (which serializes the full Spark runtime configuration to a Scala `Map`, then immediately converted to a Java `Map` by `ScalaConversionUtils.fromMap`) is called once inside `getCatalogDatasetFacet()` (line 83) and again inside `getDatasetIdentifier()` (line 130). Both methods are called for every event. The Spark conf snapshot is a full copy of all Spark properties; on a cluster with many config keys this is a non-trivial allocation.

**Root Cause**: There is no sharing of the already-extracted catalog properties map between `getCatalogDatasetFacet` and `getDatasetIdentifier`. Each builds its own copy independently.

**Code Evidence**:
```java
// getCatalogDatasetFacet() - line 82-85:
.map(conf -> conf.getAll())
.map(ScalaConversionUtils::fromMap)
.map(map -> getCatalogProperties(map, tableCatalog.name()));

// getDatasetIdentifier() - line 130-131:
Map<String, String> sparkRuntimeConfig = ScalaConversionUtils.fromMap(session.conf().getAll());
Map<String, String> catalogConf = getCatalogProperties(sparkRuntimeConfig, catalogName);
```

**Recommendation**: Extract the catalog-scoped properties snapshot once and pass it through, or cache it within the `IcebergHandler` instance for the duration of a single event. Since the `getHandlers()` call already creates a fresh `IcebergHandler` per event (see handler-list issue above), the handler instance itself could serve as a short-lived cache.

---

### [Reflection-Based Method Invocation Per Scan Event] - Severity: LOW

**Class**: `IcebergScanReportInputDatasetFacetBuilder`, `IcebergInputStatisticsInputDatasetFacetBuilder`
**Location**:
- `/home/bits/perf-audit/OpenLineage/integration/spark/vendor/iceberg/src/main/java/io/openlineage/spark/agent/vendor/iceberg/lifecycle/plan/IcebergScanReportInputDatasetFacetBuilder.java:46`
- `/home/bits/perf-audit/OpenLineage/integration/spark/vendor/iceberg/src/main/java/io/openlineage/spark/agent/vendor/iceberg/lifecycle/plan/IcebergInputStatisticsInputDatasetFacetBuilder.java:63`

**Description**: Both builders use `MethodUtils.invokeMethod(x, true, "table")` and `MethodUtils.invokeMethod(scan, true, "tasks")` respectively to access methods on the non-public `SparkBatchQueryScan` class. Apache Commons `MethodUtils.invokeMethod` with `forceAccess=true` calls `getDeclaredMethods()` to find the method and then invokes it reflectively on every call — there is no caching of the `Method` object between events.

**Root Cause**: `SparkBatchQueryScan` is a package-private class, so direct method calls are not possible without a compiled dependency. The reflection approach is unavoidable unless the Iceberg API is changed, but the resolved `Method` handle is not cached.

**Code Evidence**:
```java
// IcebergScanReportInputDatasetFacetBuilder.java:46
Table table = (Table) MethodUtils.invokeMethod(x, true, "table");

// IcebergInputStatisticsInputDatasetFacetBuilder.java:63
List<ScanTask> tasks = (List<ScanTask>) MethodUtils.invokeMethod(scan, true, "tasks");
```

**Recommendation**: Cache the resolved `java.lang.reflect.Method` objects as static fields (looked up once at class-initialization time or on first use). This avoids repeated `getDeclaredMethods()` scans on each event:
```java
private static final Method TABLE_METHOD = findMethod(SparkBatchQueryScan.class, "table");
```
Note that `tasks()` in `SparkPartitioningAwareScan` already caches its result internally (the `tasks` field is lazily initialized and then held), so the underlying file-planning I/O is not repeated — but the reflection overhead of finding and accessing the method is still incurred each time.

---

### [loadTable() on REST Catalog Always Issues HTTP Request] - Severity: MEDIUM (context-dependent)

**Class**: `IcebergHandler.getIcebergTable()` → Iceberg `RESTSessionCatalog.loadTable()`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/iceberg/IcebergHandler.java:254`

**Description**: Even with Iceberg's ETag-based table cache in `RESTSessionCatalog`, every call to `loadTable()` still sends an HTTP GET request to the REST catalog server with an `If-None-Match` header. If the server supports ETags and the table has not changed, the server returns HTTP 304 Not Modified and the cached table is returned without parsing a new response body — but a network round-trip is still required. If the REST server does not support ETags (response has no `ETag` header), the result is not cached at all and a full table metadata payload is fetched every time. The Spark-level `CachingCatalog` wraps the REST catalog only when `cache-enabled=true` (default), and its cache returns immediately without a network call — but OpenLineage calls `sparkCatalog.loadTable()` rather than going through `CachingCatalog` directly, so the caching behavior depends on Spark's SparkCatalog configuration.

**Root Cause**: Iceberg's `RESTSessionCatalog` is designed to always validate freshness via conditional HTTP GET. There is no fully offline path even with a warm cache unless the server omits the ETag header on a prior response (in which case there is no cache entry at all). The OpenLineage code calls `loadTable` twice per event (see first issue), doubling the HTTP traffic.

**Code Evidence**:
```java
// RESTSessionCatalog.java:441-449
return client
    .withAuthSession(contextualSession)
    .get(
        paths.table(identifier),   // HTTP GET /v1/.../tables/{table}
        snapshotModeToParam(mode),
        LoadTableResponse.class,
        headers,                   // contains If-None-Match: <etag> if cached
        ErrorHandlers.tableErrorHandler(),
        responseHeaders);
```

**Recommendation**: The primary fix is eliminating the duplicate `loadTable` call (see first issue). Secondarily, ensure `cache-enabled=true` in the SparkCatalog configuration so the Spark-level `CachingCatalog` sits in front of the REST client and can serve table objects from memory for subsequent loads within the same query.

---

## Clean Classes

- **`HiveCatalogTypeHandler`**: `getIdentifier()` reads a URI from config or Spark conf; no network calls. `SparkConfUtils.getMetastoreUri()` reads from already-loaded Spark configuration.
- **`HadoopCatalogTypeHandler`**: `getIdentifier()` only reads the warehouse location from the config map and constructs a path. No I/O.
- **`RestCatalogTypeHandler`**: `getIdentifier()` parses a URI from the config map. `catalogProperties()` does a string prefix check. No I/O.
- **`NessieCatalogTypeHandler`**: `getIdentifier()` parses a URI from the config map. No I/O.
- **`BigQueryMetastoreCatalogTypeHandler`**: `getIdentifier()` reads warehouse from config. `catalogProperties()` reads config keys. No I/O.
- **`BaseCatalogTypeHandler`**: `defaultTableLocation()` is pure path arithmetic. `catalogProperties()` returns an empty map. No I/O.
- **`CommitReportsFacetBuilder`**: Pure data transformation from an already-materialized `CommitReport` POJO. No I/O, no reflection.
- **`ScanReportsFacetBuilder`**: Pure data transformation from an already-materialized `ScanReport` POJO. No I/O, no reflection.
- **`IcebergCommitReportOutputDatasetFacetBuilder`**: Reads from an in-memory `CatalogMetricsReporterHolder` map keyed by snapshot ID. No I/O. The metrics data was captured asynchronously by Iceberg's own metrics reporter callback, not fetched on-demand.
- **`CatalogMetricsReporterHolder`**: Registration is guarded by a `containsKey` check so metrics reporters are not re-injected on repeated events. `getScanReportFacet()` and `getCommitReportFacet()` are in-memory linear scans over a small list (typically one catalog) — effectively O(1) in practice.

---

## Spark/Iceberg Internals Investigated

- **`/home/bits/perf-audit/iceberg/core/src/main/java/org/apache/iceberg/rest/RESTCatalog.java`**: Thin wrapper around `RESTSessionCatalog`. `loadTable()` delegates directly to the session catalog — no additional caching at this layer.
- **`/home/bits/perf-audit/iceberg/core/src/main/java/org/apache/iceberg/rest/RESTSessionCatalog.java`**: Contains the ETag-based `RESTTableCache`. On every `loadTable()` call, a conditional HTTP GET is sent. If the server returns a response without an `ETag` header, the table is not cached. If the ETag matches (HTTP 304), the cached `Supplier<BaseTable>` is returned without reparsing. The cache is keyed by `(sessionId, tableIdentifier)`.
- **`/home/bits/perf-audit/iceberg/core/src/main/java/org/apache/iceberg/CachingCatalog.java`**: Caffeine-based in-memory cache that wraps any `Catalog`. Keyed by `TableIdentifier`. When `cache-enabled=true` on the SparkCatalog, `SparkCatalog` wraps its `icebergCatalog` in `CachingCatalog.wrap()`. A hit returns the table immediately with no network call. This cache is the primary defense against repeated `loadTable` overhead for REST catalogs.
- **`/home/bits/perf-audit/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java`**: `loadTable(Identifier)` calls `icebergCatalog.loadTable()` — going through `CachingCatalog` if enabled. OpenLineage's `getIcebergTable()` calls `sparkCatalog.loadTable(identifier)`, so it does benefit from `CachingCatalog` if enabled.
- **`/home/bits/perf-audit/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkPartitioningAwareScan.java`**: `tasks()` is `synchronized` and lazily initialized — it calls `scan.planFiles()` only once and caches results in a field. Subsequent calls to `tasks()` return the cached list. Therefore `IcebergInputStatisticsInputDatasetFacetBuilder.tasks()` via reflection does not trigger repeated file planning.
- **`/home/bits/perf-audit/iceberg/core/src/main/java/org/apache/iceberg/BaseTable.java`**: `schema()`, `spec()`, `properties()`, and `currentSnapshot()` all delegate to `ops.current()`, which returns the in-memory `TableMetadata` already loaded during `loadTable()`. These are pure in-memory reads — no additional I/O once the table is loaded.
- **`/home/bits/perf-audit/iceberg/core/src/main/java/org/apache/iceberg/TableMetadata.java`**: Immutable value object. All fields (`schema`, `specs`, `properties`, snapshots) are stored in-memory collections set at construction time. Reading them is a simple field access — no I/O, no lazy loading.
