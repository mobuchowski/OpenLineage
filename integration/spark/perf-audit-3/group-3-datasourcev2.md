# Performance Audit - Group 3: DataSourceV2 Relation Dataset Builders

## Summary

The DataSourceV2 relation builders delegate all heavy work to `DataSourceV2RelationDatasetExtractor` and the catalog handler chain in `CatalogUtils3`. The most critical issues are: (1) the Iceberg and Delta catalog handlers call `catalog.loadTable()` on every invocation to extract the dataset version — these are full remote catalog round-trips that duplicate work Spark has already done during query planning; (2) `CatalogUtils3.getHandlers()` constructs a fresh handler list on every call, instantiating all catalog handler objects and performing `Class.loadClass()` for class-availability checks each time; and (3) `TableContentChangeDatasetBuilder` instantiates `new IcebergHandler(context)` four separate times in a single `isDefinedAtLogicalPlan` + `apply` pair, including two `Class.loadClass()` calls in the hot `isDefinedAtLogicalPlan` predicate path.

## Classes Audited

- `DataSourceV2RelationInputOnEndDatasetBuilder`: Input dataset builder that fires on job/SQL end events; delegates to `DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet`
- `DataSourceV2RelationInputOnStartDatasetBuilder`: Input dataset builder that fires on job/SQL start events; delegates to `DataSourceV2RelationDatasetExtractor.extractIncludingVersionFacet`
- `DataSourceV2RelationOutputDatasetBuilder`: Output dataset builder for `DataSourceV2Relation` nodes; calls extension visitor check before delegating
- `DataSourceV2ScanRelationOnEndInputDatasetBuilder`: Input dataset builder for `DataSourceV2ScanRelation`; fires on SQL execution end; iterates vendor handler factories to collect scan-level facets
- `DataSourceV2ScanRelationOnStartInputDatasetBuilder`: Input dataset builder for `DataSourceV2ScanRelation`; fires on SQL execution start
- `TableContentChangeDatasetBuilder`: Output dataset builder for DML operations (INSERT, DELETE, UPDATE, MERGE, OVERWRITE); handles multiple plan types
- `CatalogUtils3`: Central dispatcher that builds catalog handler lists and routes identifier extraction, version, and storage/catalog facet calls

---

## Performance Issues Found

### [ISSUE 1] `catalog.loadTable()` called again to extract dataset version — Severity: HIGH

**Class**: `IcebergHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/iceberg/IcebergHandler.java:242-272`

**Description**: `IcebergHandler.getDatasetVersion()` calls `getIcebergTable()`, which in turn calls `sparkCatalog.loadTable(identifier)` (or `sparkCatalog.icebergCatalog().loadTable(tableIdentifier)`). This is a full catalog `loadTable` round-trip. For remote catalogs backed by a REST, Glue, Nessie, or Hive metastore endpoint, this triggers an outbound network call.

**Root Cause**: The `DataSourceV2Relation` already holds a fully resolved `table` field (a `SparkTable` wrapping the Iceberg `Table` object). That `Table` already has a `currentSnapshot()` method available without any new catalog call. Instead of using `relation.table()` (which is already in hand), the code routes through `CatalogUtils3.getDatasetVersion()` → `IcebergHandler.getDatasetVersion()` → `getIcebergTable()` → `sparkCatalog.loadTable()`, re-fetching what Spark already fetched during query analysis. `DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation()` receives the `DataSourceV2Relation` and has `relation.table()` available but discards it, passing only `tableCatalog` and `identifier` to the catalog handler.

**Code Evidence**:
```java
// DatasetVersionDatasetFacetUtils.java:31-47
public static Optional<String> extractVersionFromDataSourceV2Relation(
    OpenLineageContext context, DataSourceV2Relation table) {
  ...
  TableCatalog tableCatalog = (TableCatalog) table.catalog().get();
  Map<String, String> tableProperties = table.table().properties();
  return CatalogUtils3.getDatasetVersion(context, tableCatalog, identifier, tableProperties);
  // NOTE: table.table() is available here but is only used for properties, not for version
}

// IcebergHandler.java:249-254
private Optional<Table> getIcebergTable(TableCatalog tableCatalog, Identifier identifier) {
  if (tableCatalog instanceof SparkCatalog) {
    SparkCatalog sparkCatalog = (SparkCatalog) tableCatalog;
    SparkTable sparkTable = (SparkTable) sparkCatalog.loadTable(identifier);  // NETWORK CALL
    return Optional.ofNullable(sparkTable.table());
  }
  ...
}
```

**Recommendation**: Pass `relation.table()` (already a `SparkTable`) through the version extraction path so that Iceberg's `currentSnapshot()` can be called directly on the already-resolved table object. The `DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation` method already has `table.table()` in scope; casting it to `SparkTable` (when it is one) and calling `.table().currentSnapshot()` avoids the second `loadTable` call entirely.

---

### [ISSUE 2] `DeltaHandler.getDatasetIdentifier()` calls `catalog.loadTable()` unconditionally — Severity: HIGH

**Class**: `DeltaHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/DeltaHandler.java:68`

**Description**: `DeltaHandler.getDatasetIdentifier()` immediately calls `catalog.loadTable(identifier)` as its first line, before any check of whether a cheaper path is available.

**Root Cause**: A `DataSourceV2Relation` already carries `table` (the `DeltaTableV2` or `V1Table`). The relation was resolved earlier in Spark's analysis phase, which already called `loadTable`. The `DeltaHandler` receives `properties` (extracted from `relation.table().properties()`) but ignores the already-loaded table object, forcing another `DeltaCatalog.loadTable()` call. For path identifiers this causes a filesystem check; for catalog-backed tables it hits the Hive metastore.

**Code Evidence**:
```java
// DeltaHandler.java:66-97
public DatasetIdentifier getDatasetIdentifier(...) {
  DeltaCatalog catalog = (DeltaCatalog) tableCatalog;
  Table table = catalog.loadTable(identifier);  // REDUNDANT LOAD
  if (catalog.isPathIdentifier(identifier)) {
    Path path = new Path(identifier.name());
    return PathUtils.fromPath(path);
  }
  ...
  if (table instanceof DeltaTableV2) {
    DeltaTableV2 deltaTable = (DeltaTableV2) table;
    Option<CatalogTable> catalogTable = deltaTable.catalogTable();
    ...
  }
  ...
}
```

**Recommendation**: Accept the already-loaded `Table` object as a parameter (the `DataSourceV2Relation.table()` reference). The `CatalogHandler` interface would need a new overload that accepts the pre-resolved table; fall back to the current `loadTable` path only when the resolved table is unavailable.

---

### [ISSUE 3] `DeltaHandler.getDatasetVersion()` calls `catalog.loadTable()` a second time — Severity: HIGH

**Class**: `DeltaHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/DeltaHandler.java:132`

**Description**: When `includeVersionFacet=true`, `DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation()` calls `CatalogUtils3.getDatasetVersion()` → `DeltaHandler.getDatasetVersion()`, which calls `deltaCatalog.loadTable(identifier)` again. This is a third `loadTable` call per event for Delta (once during Spark analysis, once in `getDatasetIdentifier`, once here).

**Root Cause**: Same as Issue 2 — the call chain strips the resolved `Table` from the relation before reaching the handler, then re-fetches it from the catalog. The `getDatasetVersion` method could derive the snapshot version directly from the `DeltaTableV2` already in hand via the `DeltaLog` that backs it.

**Code Evidence**:
```java
// DeltaHandler.java:128-143
public Optional<String> getDatasetVersion(
    TableCatalog tableCatalog, Identifier identifier, Map<String, String> properties) {
  DeltaCatalog deltaCatalog = (DeltaCatalog) tableCatalog;
  Table table = deltaCatalog.loadTable(identifier);  // SECOND/THIRD loadTable CALL
  if (table instanceof DeltaTableV2) {
    DeltaTableV2 deltaTable = (DeltaTableV2) table;
    Optional<Snapshot> snapshot = getDeltaTableSnapshot(deltaTable);
    ...
  }
}
```

**Recommendation**: Same as Issue 2 — pass the already-resolved table through the version path. The `DeltaLog` or `TahoeLogFileIndex` that provides the snapshot is already accessible from the `DeltaTableV2` present in `relation.table()`.

---

### [ISSUE 4] `CatalogUtils3.getHandlers()` reconstructs the full handler list on every call — Severity: MEDIUM

**Class**: `CatalogUtils3`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/CatalogUtils3.java:26-36`

**Description**: `getHandlers(context)` is a static method that instantiates six handler objects (`IcebergHandler`, `DeltaHandler`, `DatabricksDeltaHandler`, `DatabricksUnityV2Handler`, `JdbcHandler`, `V2SessionCatalogHandler`) and then filters them via `CatalogHandler::hasClasses` on every call. `hasClasses()` for `DeltaHandler`, `DatabricksDeltaHandler`, `DatabricksUnityV2Handler`, and `IcebergHandler` each call `ClassLoader.loadClass()`. `getHandlers()` is called from `getDatasetIdentifier()`, `getCatalogHandler()`, and `getDatasetVersion()` — so a single `extract()` call in the extractor (which calls both `getDatasetIdentifier` + `getDatasetVersion` + `addStorageAndCatalogFacets`) triggers this reconstruction three times.

**Root Cause**: The handler list is context-dependent (takes `OpenLineageContext`) but the class availability results are static facts that do not change at runtime. The current design makes it impossible to statically cache the list.

**Code Evidence**:
```java
// CatalogUtils3.java:26-36
private static List<CatalogHandler> getHandlers(OpenLineageContext context) {
  List<CatalogHandler> handlers =
      Arrays.asList(
          new IcebergHandler(context),       // allocates List of 6 BaseCatalogTypeHandler
          new DeltaHandler(context),
          new DatabricksDeltaHandler(context),
          new DatabricksUnityV2Handler(context),
          new JdbcHandler(context),          // allocates DatasetNamespaceCombinedResolver
          new V2SessionCatalogHandler());
  return handlers.stream().filter(CatalogHandler::hasClasses).collect(Collectors.toList());
  // hasClasses() calls ClassLoader.loadClass for 4 of the 6 handlers above
}

// Called from:
// getDatasetIdentifier() line 48
// getCatalogHandler() line 76
// (getCatalogHandler is called again from getStorageDatasetFacet, getCatalogDatasetFacet, getDatasetVersion)
```

**Recommendation**: Cache the set of available handler class names as static boolean flags (computed once at class initialization). The context is only needed by handler methods, not for availability checks. Consider separating `hasClasses()` (static, cached) from handler construction, or storing a pre-filtered handler type list and only wrapping with `context` when a handler is actually needed.

---

### [ISSUE 5] `TableContentChangeDatasetBuilder` instantiates `new IcebergHandler(context)` four times per plan — Severity: MEDIUM

**Class**: `TableContentChangeDatasetBuilder`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/TableContentChangeDatasetBuilder.java:51-54,102-105`

**Description**: `isDefinedAtLogicalPlan()` creates two separate `new IcebergHandler(context)` instances and calls `.hasClasses()` on each. When the plan is accepted and `apply()` is called, `getNamedRelation()` creates two more `new IcebergHandler(context)` instances for the same `hasClasses()` checks. Each instantiation builds a six-element `catalogTypeHandlers` list. Each `hasClasses()` call loads the `org.apache.iceberg.catalog.Catalog` class.

**Root Cause**: There is no shared state for the Iceberg class-availability check. The method that checks this (`isDefinedAtLogicalPlan`) is called for every node in the logical plan tree during traversal, meaning this fires many times per event.

**Code Evidence**:
```java
// TableContentChangeDatasetBuilder.java:46-55
public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
  return (x instanceof OverwriteByExpression)
      || (x instanceof OverwritePartitionsDynamic)
      || (x instanceof DeleteFromTable)
      || (x instanceof UpdateTable)
      || (new IcebergHandler(context).hasClasses() && x instanceof ReplaceData)  // ALLOC #1
      || (new IcebergHandler(context).hasClasses() && x instanceof WriteDelta)   // ALLOC #2
      || (x instanceof MergeIntoTable)
      || (x instanceof InsertIntoStatement);
}

// TableContentChangeDatasetBuilder.java:102-105
} else if (new IcebergHandler(context).hasClasses() && x instanceof ReplaceData) {  // ALLOC #3
  return ((ReplaceData) x).table();
} else if (new IcebergHandler(context).hasClasses() && x instanceof WriteDelta) {   // ALLOC #4
  return ((WriteDelta) x).table();
```

**Recommendation**: Cache the `IcebergHandler.hasClasses()` result as a static final boolean field set once at class load time, or compute it once in the constructor and store it as an instance field. The `isDefinedAtLogicalPlan` hot path should not allocate handler objects.

---

### [ISSUE 6] `IcebergHandler.getDatasetIdentifier()` calls `session.conf().getAll()` unconditionally — Severity: MEDIUM

**Class**: `IcebergHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/iceberg/IcebergHandler.java:130`

**Description**: `getDatasetIdentifier()` calls `ScalaConversionUtils.fromMap(session.conf().getAll())`. `SparkSession.conf().getAll()` materializes the entire Spark configuration as a `scala.collection.immutable.Map`, which is then converted to a `java.util.Map`. In large deployments with hundreds of Spark configuration entries, this copies the entire config map on every dataset extraction call.

Additionally, `getCatalogDatasetFacet()` at line 83 makes the same `conf.getAll()` call again independently:
```java
.map(conf -> conf.getAll())
.map(ScalaConversionUtils::fromMap)
.map(map -> getCatalogProperties(map, tableCatalog.name()));
```

This means `conf.getAll()` is called at least twice per Iceberg dataset per event — once in `getDatasetIdentifier()` and once in `getCatalogDatasetFacet()`.

**Root Cause**: No config map is cached across the two method calls. Since Spark configuration is effectively immutable after session start, the full-copy `getAll()` call is wasteful.

**Code Evidence**:
```java
// IcebergHandler.java:130-131
Map<String, String> sparkRuntimeConfig = ScalaConversionUtils.fromMap(session.conf().getAll());
Map<String, String> catalogConf = getCatalogProperties(sparkRuntimeConfig, catalogName);

// IcebergHandler.java:79-86
Optional<Map<String, String>> catalogConf =
    context.getSparkSession()
        .map(SparkSession::conf)
        .map(conf -> conf.getAll())          // full config copy #1
        .map(ScalaConversionUtils::fromMap)
        .map(map -> getCatalogProperties(map, tableCatalog.name()));
```

**Recommendation**: Extract the catalog-specific property subset once and cache it keyed by catalog name in the `OpenLineageContext` or as a field in the handler instance. The full `conf.getAll()` call should not be repeated on every dataset extraction.

---

### [ISSUE 7] `V2SessionCatalogHandler.getDatasetIdentifier()` calls `v2Catalog.loadNamespaceMetadata()` — Severity: MEDIUM

**Class**: `V2SessionCatalogHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/V2SessionCatalogHandler.java:41`

**Description**: `getDatasetIdentifier()` always calls `v2Catalog.loadNamespaceMetadata(identifier.namespace())`. As seen in the Spark source (`V2SessionCatalog.scala:393-406`), this calls `catalog.getDatabaseMetadata(db)` on the Hive metastore client. For queries with many input tables, this fires once per table per event.

**Root Cause**: The method needs the namespace location to construct the table path, but this information is often stable and could be cached (namespaces rarely change during a Spark session). There is no memoization of namespace lookups.

**Code Evidence**:
```java
// V2SessionCatalogHandler.java:39-41
V2SessionCatalog v2Catalog = (V2SessionCatalog) tableCatalog;
Map<String, String> namespaceMetadata = v2Catalog.loadNamespaceMetadata(identifier.namespace());
// V2SessionCatalog delegates to: catalog.getDatabaseMetadata(db).toMetadata — Hive metastore call
```

**Recommendation**: Cache namespace metadata in a `Map<String, Map<String, String>>` keyed by namespace within `OpenLineageContext` or in the handler. The metastore does not change during a Spark session for a given namespace.

---

### [ISSUE 8] `AbstractDatabricksHandler.getDatasetIdentifier()` calls `tableCatalog.loadTable()` as a fallback when location is absent from properties — Severity: LOW

**Class**: `AbstractDatabricksHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/AbstractDatabricksHandler.java:88-98`

**Description**: When neither path identifier nor the `location` property is present in `properties`, the handler falls back to calling `tableCatalog.loadTable(identifier)` to retrieve table properties from the catalog. This is a Databricks-proprietary catalog call.

**Root Cause**: The `location` property is absent for some Databricks table configurations. The fallback is conditional (only when needed), so severity is lower. However, the `relation.table()` object is already available in the caller and carries the same properties.

**Code Evidence**:
```java
// AbstractDatabricksHandler.java:88-98
if (!location.isPresent()) {
  try {
    location =
        Optional.ofNullable(tableCatalog.loadTable(identifier))  // CATALOG CALL
            .map(t -> t.properties())
            .filter(p -> p.containsKey("location"))
            .map(p -> p.get("location"));
  } catch (NoSuchTableException e) { }
}
```

**Recommendation**: The caller (`DataSourceV2RelationDatasetExtractor`) already reads `relation.table().properties()` before calling into the catalog handler. Pass the resolved `Table` object through to the handler to avoid the secondary `loadTable`.

---

### [ISSUE 9] Reflection-based `isDefinedAt` dispatch in `SparkOpenLineageExtensionVisitorWrapper` is called per-table in hot path — Severity: LOW

**Class**: `SparkOpenLineageExtensionVisitorWrapper`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/SparkOpenLineageExtensionVisitorWrapper.java:85-103`

**Description**: `context.getSparkExtensionVisitorWrapper().isDefinedAt(relation.table())` is called in `DataSourceV2RelationOutputDatasetBuilder.apply()`, `DataSourceV2ScanRelationOnEndInputDatasetBuilder.apply()`, and `DataSourceV2ScanRelationOnStartInputDatasetBuilder.apply()`. The `isDefinedAt` implementation uses reflection (`classInstance.getClass().getMethod(...)`) to find the method on each call, and no `Method` object is cached between calls.

**Root Cause**: `getMethod()` calls `classInstance.getClass().getMethod(methodName, parameterTypes)` every invocation. While JVM reflection method lookup is partially cached by the JVM, this still allocates `ImmutablePair` objects and traverses a stream for each call.

**Code Evidence**:
```java
// SparkOpenLineageExtensionVisitorWrapper.java:85-103
public boolean isDefinedAt(Object object) {
  return hasLoadedObjects
      && extensionObjects.stream()
          .map(o -> getMethod(o, "isDefinedAt", Object.class))  // reflection lookup per call
          ...
}

private Optional<ImmutablePair<Object, Method>> getMethod(...) {
  Method method = classInstance.getClass().getMethod(methodName, parameterTypes);  // no cache
  ...
}
```

**Recommendation**: Cache `Method` objects in a `Map<String, Method>` keyed by method name at construction time, since the extension objects and their methods are fixed after initialization. The `hasLoadedObjects` short-circuit already handles the common empty case efficiently.

---

## Clean Classes

- **`DataSourceV2RelationInputOnEndDatasetBuilder`**: This class itself is clean — it is a thin wrapper that correctly delegates to the extractor. The `isDefinedAtLogicalPlan` uses a plain `instanceof` check. Its only overhead comes from the downstream extractor and catalog handler chain, documented above.

- **`DataSourceV2RelationInputOnStartDatasetBuilder`**: Clean — same pattern as the End builder. No issues at this layer.

- **`DataSourceV2ScanRelationOnStartInputDatasetBuilder`**: Clean at this layer. Delegates correctly, `isDefinedAtLogicalPlan` uses `instanceof`. The vendor handler factory loop in the corresponding `OnEnd` builder could accumulate overhead if many vendors are registered, but is not inherently problematic.

- **`CosmosHandler`**: Clean. Identifier extraction is pure string parsing on `relation.table().name()` — no catalog calls, no reflection, no I/O.

- **`JdbcHandler`**: Mostly clean. Uses reflection (`FieldUtils.readField`) once to read the `JDBCOptions` from a `JDBCTableCatalog` private field, but this is a one-time read of an in-process object, not a network call. No catalog round-trips.

- **`ExtensionDataSourceV2Utils`**: Clean. Property lookups and JSON deserialization are local operations. The static `predefinedFacets` map is initialized once.

---

## Spark/Iceberg Internals Investigated

**`DataSourceV2Relation.scala`** (`/home/bits/perf-audit/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceV2Relation.scala`):
- Key finding: `DataSourceV2Relation` is a case class holding `table: Table`, `catalog: Option[CatalogPlugin]`, `identifier: Option[Identifier]` as **constructor parameters** (not lazy vals). The `table` field is fully resolved at construction time by Spark's analysis phase — it is the live, already-loaded catalog table object. Accessing `relation.table()` is a direct field read with no catalog I/O.
- `DataSourceV2ScanRelation` similarly wraps a fully-materialized `DataSourceV2Relation` and a resolved `Scan`. No lazy fetching occurs on field access.
- `computeStats()` *would* trigger a `newScanBuilder().build()` call, but OpenLineage does not call `computeStats()`, so this is not an issue.

**Iceberg `SparkCatalog.java`** (`/home/bits/perf-audit/iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java`):
- Key finding: `SparkCatalog.loadTable(Identifier)` calls `icebergCatalog.loadTable(buildIdentifier(ident))`. The underlying `icebergCatalog` is the real Iceberg catalog (REST, Glue, Hive, Nessie, Hadoop). For REST, Glue, and Nessie catalogs this is a synchronous HTTP call to the catalog service. Spark does not cache the result of `loadTable` inside `SparkCatalog` itself when `cacheEnabled=false` (the default via `!cacheEnabled` passed to `SparkTable` constructor). Even when `cacheEnabled=true`, the catalog-level cache is the Iceberg `CachingCatalog`, which may or may not be configured. Therefore, calling `loadTable` again from OpenLineage cannot assume a cache hit.
- The `SparkTable` returned by `loadTable` wraps the Iceberg `Table` object and its `currentSnapshot()` is a local in-memory operation (reads from the `TableMetadata` already loaded). This means once the `SparkTable` is in hand, getting the snapshot version is cheap — the expense is entirely in the `loadTable()` call itself.

**Spark `V2SessionCatalog.scala`** (`/home/bits/perf-audit/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/V2SessionCatalog.scala`):
- Key finding: `loadNamespaceMetadata(namespace)` calls `catalog.getDatabaseMetadata(db).toMetadata`. `catalog` here is Spark's `SessionCatalog`, which delegates to the Hive metastore client for Hive-backed sessions. This is a synchronous RPC to the Hive metastore for every namespace lookup. There is a Spark-level cache for table metadata (`getCachedTable`) but `getDatabaseMetadata` (namespace level) does not benefit from it.
- `loadTable(ident)` calls `catalog.getTableMetadata(ident.asTableIdentifier)` which is also a Hive metastore call, though it goes through Spark's table cache for some paths.
