# Performance Audit - Group 2: LogicalRelation and DataSourceV1 Builders

## Summary

The LogicalRelation and DataSourceV1 builders contain several significant performance issues that fire on every lineage event. The most severe is an unconditional filesystem call (`isFile()`) inside `isSingleFileRelation` that triggers an HDFS/S3/GCS round-trip every time a `HadoopFsRelation` with a single root path is processed. A second critical issue is the `hasDeltaClasses()` method in `DatasetVersionDatasetFacetUtils` which calls `ClassLoader.loadClass()` on every invocation without caching the result, causing repeated class-loading overhead on every event for Delta tables. Reflection calls (`FieldUtils.readField`, `MethodUtils.invokeMethod`) are also used on hot paths for JDBC catalog and identifier resolution, adding unnecessary JVM overhead per event.

## Classes Audited

- `LogicalRelationDatasetBuilder` (shared): Main dispatcher for `LogicalRelation` nodes; routes to HadoopFs, JDBC, or CatalogTable handlers
- `LogicalRelationDatasetBuilder` (spark3): Spark3 extension adding Delta version extraction and catalog/storage facet generation via reflection
- `InsertIntoHadoopFsRelationVisitor` (shared): Handles write commands to Hadoop FS; delegates to `PathUtils`
- `JdbcRelationHandler` (shared): Handles JDBC reads; delegates SQL parsing to `JdbcSparkUtils` via JNI
- `JdbcHandler` (spark3 catalog): Resolves JDBC dataset identifiers for V2 catalog; uses `FieldUtils.readField` to access private `options` field
- `HadoopFsRelation` (Spark): Value class; `inputFiles` delegates to `location.inputFiles` which on `InMemoryFileIndex` scans all leaf files; `sizeInBytes` delegates to `location.sizeInBytes` which sums all file lengths
- `InMemoryFileIndex` (Spark): Performs full directory listing via `listLeafFiles` on construction; caches results but `inputFiles` iterates cached leaf file set
- `CatalogFileIndex` (Spark): `inputFiles` calls `filterPartitions(Nil)` which creates a new `InMemoryFileIndex` and triggers `listLeafFiles`
- `PartitioningAwareFileIndex` (Spark): `inputFiles` materializes all leaf file paths from in-memory cache
- `DatasetVersionDatasetFacetUtils` (spark3): Extracts Delta snapshot version from `LogicalRelation`; calls `ClassLoader.loadClass` on every event

## Performance Issues Found

### Unconditional Filesystem `isFile()` Check - Severity: HIGH
**Class**: `LogicalRelationDatasetBuilder` (shared)
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/LogicalRelationDatasetBuilder.java:272-283`
**Description**: Every time a `HadoopFsRelation` has exactly one root path, `isSingleFileRelation()` calls `path.getFileSystem(hadoopConfig).isFile(path)`. This materializes a `FileSystem` instance from the Hadoop registry (or creates one) and then issues a remote metadata call (`FileStatus` fetch) against HDFS, S3, GCS, or ADLS. This happens on every lineage event for single-path relations.
**Root Cause**: The method is designed to distinguish a single file from a single directory, but there is no caching of this result. The `HadoopFsRelation` already holds a `FileIndex` which knows whether root paths are files or directories (the `InMemoryFileIndex` populates `leafFiles` vs `leafDirToChildrenFiles` at construction time), but this information is not consulted.
**Code Evidence**:
```java
private boolean isSingleFileRelation(Collection<Path> paths, Configuration hadoopConfig) {
    if (paths.size() != 1) {
        return false;
    }
    try {
        Path path = paths.stream().findFirst().get();
        return path.getFileSystem(hadoopConfig).isFile(path);  // REMOTE CALL
    } catch (IOException e) {
        return false;
    }
}
```
`path.getFileSystem(hadoopConfig).isFile(path)` maps to `FileSystem.getStatus(path).isFile()` which is a blocking network call to the remote filesystem.
**Recommendation**: Inspect the `FileIndex` type already held by the `HadoopFsRelation`. For an `InMemoryFileIndex`, check its cached `leafFiles` map: if the path is a key in `leafFiles` it is a file; if it is in `leafDirToChildrenFiles` it is a directory. This avoids any remote call. If the `FileIndex` type is unknown, a fallback to the current check is acceptable but should only occur once (the result could be cached on the relation object or a companion map keyed by path string).

---

### `relation.inputFiles()` Call Materializes All Leaf Files - Severity: HIGH
**Class**: `LogicalRelationDatasetBuilder` (shared)
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/LogicalRelationDatasetBuilder.java:201-214`
**Description**: In `handleHadoopFsRelation`, `relation.inputFiles()` is called to obtain a count of input files. This method on `HadoopFsRelation` delegates to `location.inputFiles` on the underlying `FileIndex`. For `PartitioningAwareFileIndex` (including `InMemoryFileIndex`), `inputFiles` calls `allFiles()` which iterates the entire cached leaf file map and URL-encodes every path. For `CatalogFileIndex`, it is worse: `inputFiles` calls `filterPartitions(Nil)`, which creates a brand-new `InMemoryFileIndex` and triggers `listLeafFiles` — a full recursive filesystem listing from scratch. The file count needed is just a single integer, but the code materialises all file paths to count them.
**Root Cause**: OpenLineage only needs the file count (`long`), but calls `inputFiles()` which returns the full `Array[String]` of all file paths. Even for `InMemoryFileIndex`, this allocates a new String array of N URL-encoded path strings just to call `.length` on it. For `CatalogFileIndex`, the cost is a full HMS partition listing plus filesystem listing.
**Code Evidence**:
```java
if (relation.inputFiles() != null) {
    datasetFacetsBuilder
        .getInputFacets()
        .inputStatistics(
            context.getOpenLineage()
                .newInputStatisticsInputDatasetFacetBuilder()
                .size(relation.sizeInBytes())
                .fileCount(
                    Optional.of(relation.inputFiles())  // SECOND CALL HERE
                        .map(l -> l.length)
                        .map(Long::valueOf)
                        .orElse(0L))
                .build());
}
```
`inputFiles()` is called **twice** on line 201 (null check) and line 210 (value use). Each call to `PartitioningAwareFileIndex.inputFiles` allocates a full String array. Each call to `CatalogFileIndex.inputFiles` triggers a new `listLeafFiles` scan.
**Recommendation**: Call `relation.inputFiles()` exactly once, store the result in a local variable, then use that variable for both the null check and the `.length` call. More fundamentally, consider using `relation.location().sizeInBytes` and a cheaper file-count path: for `InMemoryFileIndex`, the cached `leafFiles` map size is available without any IO or String allocation. The two-call pattern is an immediate easy win.

---

### `hasDeltaClasses()` Class-Loading on Every Event - Severity: HIGH
**Class**: `DatasetVersionDatasetFacetUtils` (spark3)
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/utils/DatasetVersionDatasetFacetUtils.java:70-81`
**Description**: `extractVersionFromLogicalRelation` is called for every `LogicalRelation` event in Spark3. When the relation is a `HadoopFsRelation` over a Delta table, it calls `hasDeltaClasses()` which invokes `ClassLoader.loadClass("org.apache.spark.sql.delta.files.TahoeLogFileIndex")` on every single invocation. Class loading is not free: even for cached classes it still has to walk the class loader hierarchy. The method has no static caching of its result.
**Root Cause**: The method is a guard to avoid `NoClassDefFoundError` when Delta is not on the classpath, but it is implemented as a fresh class-load attempt every time rather than computing the result once at startup and caching a boolean.
**Code Evidence**:
```java
protected static boolean hasDeltaClasses() {
    try {
        io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils.class
            .getClassLoader()
            .loadClass("org.apache.spark.sql.delta.files.TahoeLogFileIndex");  // EVERY CALL
        return true;
    } catch (NoClassDefFoundError | Exception e) {
    }
    return false;
}
```
This is called from `extractVersionFromLogicalRelation`, which is called from `getDatasetVersion` inside `LogicalRelationDatasetBuilder.apply()` — the hot path for every lineage event.
**Recommendation**: Cache the result in a `static final boolean HAS_DELTA_CLASSES` field initialized in a static initializer or via a `static final` lazy holder. The check needs to happen only once per JVM lifetime. The pattern is already used elsewhere in the codebase (`OpenLineageSql.loadError` is set once in a static block).

---

### Reflection on Every Catalog Identifier Resolution - Severity: MEDIUM
**Class**: `LogicalRelationDatasetBuilder` (spark3)
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/LogicalRelationDatasetBuilder.java:67`
**Description**: In `addCatalogAndStorageFacets`, `MethodUtils.invokeMethod(catalogTable.identifier(), "catalog")` is called to retrieve the catalog name from the table identifier. `MethodUtils.invokeMethod` from Apache Commons Lang performs method lookup via reflection on every call: it searches the method by name in the class hierarchy, creates an `AccessibleObject`, and invokes it dynamically. This fires for every catalogued table event.
**Root Cause**: Spark's `TableIdentifier.catalog()` method was added in Spark 3.x and is being called reflectively for cross-version compatibility, but there is no caching of the reflected `Method` object. Apache Commons `MethodUtils` does not cache lookups internally.
**Code Evidence**:
```java
catalogName =
    ((Option<String>) MethodUtils.invokeMethod(catalogTable.identifier(), "catalog")).get();
```
`MethodUtils.invokeMethod` internally calls `getMatchingAccessibleMethod` which scans method arrays using string comparison.
**Recommendation**: Cache the `java.lang.reflect.Method` object in a `static final` field (initialized once with `getDeclaredMethod` and `setAccessible(true)`), then call `method.invoke(identifier)` directly. This eliminates the per-event method lookup overhead. Alternatively, if the minimum supported Spark version has `TableIdentifier.catalog()` as a direct API, call it directly without reflection.

---

### Reflection via `FieldUtils.readField` for JDBC Options - Severity: MEDIUM
**Class**: `JdbcHandler` (spark3 catalog)
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/JdbcHandler.java:89-98`
**Description**: `getJdbcOptions` accesses the private `options` field of `JDBCTableCatalog` via `FieldUtils.readField(catalog, "options", true)` on every call. This is called from both `getDatasetIdentifier` and `getCatalogDatasetFacet`, so a single JDBC V2 catalog event triggers two reflective field reads. `FieldUtils.readField` with `forceAccess=true` calls `Field.setAccessible(true)` on every invocation, which in Java 9+ triggers security checks and may cause `InaccessibleObjectException` warnings.
**Root Cause**: `JDBCTableCatalog.options` is a private field with no public accessor. The field is accessed reflectively to avoid depending on internal Spark API. However, `setAccessible(true)` should be called once and cached on the `Field` object, not repeated each time.
**Code Evidence**:
```java
private Optional<JDBCOptions> getJdbcOptions(TableCatalog tableCatalog) {
    try {
        JDBCTableCatalog catalog = (JDBCTableCatalog) tableCatalog;
        JDBCOptions jdbcOptions = (JDBCOptions) FieldUtils.readField(catalog, "options", true);
        // FieldUtils.readField always calls field.setAccessible(true) internally
        return Optional.of(jdbcOptions);
    } catch (IllegalAccessException e) {
        ...
    }
}
```
**Recommendation**: Cache the `java.lang.reflect.Field` object in a `static final` field. Call `setAccessible(true)` once during static initialization, then use `field.get(catalog)` directly on the hot path. This also prevents repeated security-manager checks per event. A further improvement would be to cache the resolved `JDBCOptions` per catalog instance (since the catalog object itself is stable across events for the same table).

---

### `newHadoopConfWithOptions` Creates New Configuration Object Per Event - Severity: MEDIUM
**Class**: `LogicalRelationDatasetBuilder` (shared)
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/LogicalRelationDatasetBuilder.java:186-187`
**Description**: In `handleHadoopFsRelation`, `session.sessionState().newHadoopConfWithOptions(relation.options())` is called on every event. This creates a full deep copy of the session's Hadoop `Configuration` object and then iterates the relation's options map to overlay them. Hadoop `Configuration` copies can be expensive: the base configuration has hundreds of keys and cloning it involves copying all internal hash maps.
**Root Cause**: OpenLineage needs the Hadoop configuration to call `path.getFileSystem(hadoopConfig)` in `isSingleFileRelation` and `PlanUtils.getDirectoryPaths`. The configuration is used solely to obtain a `FileSystem` instance for path resolution. Creating a full copy for this purpose is wasteful when the `FileIndex` on the relation already has a Hadoop conf embedded in it.
**Code Evidence**:
```java
Configuration hadoopConfig =
    session.sessionState().newHadoopConfWithOptions(relation.options());
```
`newHadoopConfWithOptions` (SessionState.scala:121-129) calls `newHadoopConf()` (which clones `sparkContext.hadoopConfiguration`) and then copies each option from the map. This fires on every `HadoopFsRelation` event.
**Recommendation**: The `PartitioningAwareFileIndex` (the typical `FileIndex` implementation) already holds its own `hadoopConf` built with the same options (it calls `sparkSession.sessionState.newHadoopConfWithOptions(parameters)` during construction). Access that conf via `relation.location()` cast to `PartitioningAwareFileIndex` where possible, or access `sparkContext.hadoopConfiguration` directly and apply only the necessary options. For the `isSingleFileRelation` check specifically, the filesystem check can be eliminated entirely (see that issue above), which removes the need for the configuration copy on the single-file branch.

---

### JNI SQL Parsing on Every Complex JDBC Query - Severity: MEDIUM
**Class**: `JdbcRelationHandler` / `JdbcSparkUtils`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/JdbcSparkUtils.java:86`
**Description**: For JDBC relations that use a subquery or JOIN syntax in `dbtable`, `JdbcSparkUtils.extractQueryFromSpark` calls `OpenLineageSql.parse(...)`, which crosses the JNI boundary into a native Rust library on every invocation. JNI calls have non-trivial overhead (parameter marshaling, call frame setup, GC safepoint coordination). The parse result for a given SQL string is never cached; if the same query appears in multiple events (e.g., repeated job runs), the Rust parser is invoked each time.
**Root Cause**: There is no memoization layer over `OpenLineageSql.parse`. The JDBC `dbtable` string is stable for a given `JDBCRelation` object, but the relation object may be reconstructed on each plan visit. Results are not cached by query string.
**Code Evidence**:
```java
String dialect = extractDialectFromJdbcUrl(relation.jdbcOptions().url());
Optional<SqlMeta> sqlMeta = OpenLineageSql.parse(Collections.singletonList(query), dialect);
```
`OpenLineageSql.parse` is declared `native` — it is a JNI call into `libopenlineage_sql_java.so`.
**Recommendation**: Add a bounded `ConcurrentHashMap<String, Optional<SqlMeta>>` cache keyed by `(query, dialect)` string pair. Since the SQL text and dialect are both stable strings, the cache can be a simple static `LoadingCache` (Guava) with a small size bound (e.g., 256 entries) and no expiry. This eliminates redundant JNI crossings for repeated queries across job runs.

---

### `PlanUtils.getDirectoryPath` Issues Filesystem `getFileStatus` Per Path - Severity: MEDIUM
**Class**: `PlanUtils`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/PlanUtils.java:278-289`
**Description**: For multi-path `HadoopFsRelation` cases (where `isSingleFileRelation` returns false), `PlanUtils.getDirectoryPaths` is called with each root path. Inside, `getDirectoryPath` calls `p.getFileSystem(hadoopConf).getFileStatus(p).isFile()` for each path. This is the same pattern as `isSingleFileRelation` but applied to N paths: N blocking filesystem metadata calls (one `getFileStatus` per path) on each lineage event.
**Root Cause**: `getDirectoryPath` is trying to resolve whether each root path points to a file or a directory so it can return the parent directory. However, the `InMemoryFileIndex` already has this information cached in `leafFiles` (a map of file path -> FileStatus). If a path is in `leafFiles`, it is a file; if it is in `leafDirToChildrenFiles`, it is a directory. This metadata was already fetched when Spark built the relation and requires no additional IO.
**Code Evidence**:
```java
private static Path getDirectoryPath(Path p, Configuration hadoopConf) {
    try {
        if (p.getFileSystem(hadoopConf).getFileStatus(p).isFile()) {  // REMOTE CALL
            return p.getParent();
        } else {
            return p;
        }
    } catch (IOException e) {
        log.warn("Unable to get file system for path: {}", e.getMessage());
        return p;
    }
}
```
**Recommendation**: Pass the `FileIndex` (or the pre-fetched `FileStatus` map) into `getDirectoryPaths` so that path classification can be answered from in-memory data. If the `FileIndex` is an `InMemoryFileIndex`, check `leafFiles.containsKey(qualifiedPath)` to determine whether the path is a file. Fall back to the current `getFileStatus` call only when the `FileIndex` type is unknown.

## Clean Classes

**`InsertIntoHadoopFsRelationVisitor`**: This class is clean. It delegates identifier construction to `PathUtils.fromCatalogTable` or `PathUtils.fromPath`, neither of which triggers IO. The `command.query().schema()` call is a pure in-memory plan attribute access. The `SaveMode` check and dataset construction are all O(1) in-memory operations. No reflection, no network calls, no iteration over large collections.

**`JdbcRelationHandler`** (the thin wrapper class itself): The class is a minimal dispatcher. Its `handleRelation` and `getDatasets` methods do nothing except delegate to `JdbcSparkUtils`. The wrapper itself introduces no overhead. The substantive cost lives in `JdbcSparkUtils.extractQueryFromSpark`.

**`JdbcSparkUtils.extractQueryFromSpark` (simple table name path)**: When `dbtable` is a plain table name (no spaces, no subquery), the fast path at line 63-77 of `JdbcSparkUtils` constructs `SqlMeta` directly from in-memory data using `relation.schema().fields()` iteration. This avoids the JNI parser entirely and is proportional only to the number of columns in the schema — which is acceptable for lineage purposes.

## Spark/Iceberg Internals Investigated

**`HadoopFsRelation.scala`**: `sizeInBytes` delegates to `location.sizeInBytes * compressionFactor`. `inputFiles` delegates to `location.inputFiles`. Both are computed by the `FileIndex` implementation. The `schema` field is computed once at construction via `PartitioningUtils.mergeDataAndPartitionSchema` — pure in-memory work.

**`InMemoryFileIndex.scala`**: Critical finding: `listLeafFiles` is called in the constructor (`refresh0()` at line 70), so by the time OpenLineage sees a `HadoopFsRelation`, the file listing has already been done and is cached in `cachedLeafFiles` and `cachedLeafDirToChildrenFiles`. Calling `inputFiles` on `InMemoryFileIndex` iterates this in-memory cache (no new IO), but does allocate a new String array of URL-encoded paths. The `sizeInBytes` implementation sums `getLen()` on each cached `FileStatus` — no IO.

**`CatalogFileIndex.scala`**: Critical finding: unlike `InMemoryFileIndex`, `CatalogFileIndex` does NOT pre-populate a leaf file cache. Its `inputFiles` method (line 97) calls `filterPartitions(Nil)` which constructs a new `InMemoryFileIndex` and triggers a fresh `listLeafFiles` scan. This means any code path that calls `relation.inputFiles()` on a relation backed by a `CatalogFileIndex` (typical for Hive/HMS catalog tables) will perform a full recursive filesystem listing. OpenLineage's `handleHadoopFsRelation` calls `relation.inputFiles()` in the null-check guard, which can trigger this for every HMS-backed table read event.

**`PartitioningAwareFileIndex.scala`**: `inputFiles` (line 114-115) calls `allFiles()` which returns `leafFiles.values.toSeq` or iterates `leafDirToChildrenFiles` from the in-memory cache. The URL encoding (`SparkPath.fromFileStatus(fs).urlEncoded`) happens per file. For a table with thousands of files, this is a large allocation per event but no IO. `sizeInBytes` (line 117) sums file lengths from the same in-memory cache.

**`SessionState.scala` (`newHadoopConfWithOptions`)**: Confirmed to deep-copy `sparkContext.hadoopConfiguration` on every call (creates a new `Configuration` object and calls `set` for each option). For a `HadoopFsRelation` with many options, this copy is proportional to the sum of the base conf size plus options count.

**`JDBCRelation.scala`**: The `schema` field is pre-resolved at construction time (line 263-264) via `JDBCRDD.resolveTable` — a network call to the remote database — but this happens only once when the relation is created, not repeated by OpenLineage. OpenLineage accesses `relation.schema()` which returns the already-resolved cached field. No JDBC connection is opened by OpenLineage's `JdbcRelationHandler`.

**`JDBCOptions.scala`**: `asConnectionProperties` is a `val` (computed once on construction). `url` and `tableOrQuery` are also `val`. Accessing these from OpenLineage is O(1) field reads — no repeated computation.
