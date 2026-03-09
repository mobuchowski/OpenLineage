# Performance Audit - Group 9: Hive Visitors and Handlers

## Summary

The Hive visitors and handlers in OpenLineage are generally well-behaved in that they read from `CatalogTable` objects that are already fully resolved (schema, storage format, location) before the visitor is invoked. However, there are two significant performance concerns: (1) `PathUtils.fromCatalogTable` unconditionally calls `SessionCatalog.defaultTablePath` when a table has no explicit storage location, which chains into `getDatabaseMetadata` -> `HiveExternalCatalog.getDatabase` -> a live HMS Thrift RPC; and (2) `InsertIntoHiveTableVisitor` calls `PathUtils.fromCatalogTable` up to five times per plan invocation (four branches in `apply` plus one in `jobNameSuffix`), multiplying any HMS latency. `V2SessionCatalogHandler` additionally triggers an HMS `getDatabase` RPC via `loadNamespaceMetadata` on every invocation.

## Classes Audited

- `HiveTableRelationVisitor`: Reads schema and location from an already-resolved `HiveTableRelation.tableMeta`; delegates to `PathUtils.fromCatalogTable`
- `InsertIntoHiveTableVisitor`: Extracts `CatalogTable` from `InsertIntoHiveTable.table()`; calls `PathUtils.fromCatalogTable` redundantly up to 5 times
- `InsertIntoHiveDirVisitor`: Reads only `storage().locationUri()` (a plain `Option[URI]`) and `query().schema()` from an already-analysed plan; no HMS calls
- `CreateHiveTableAsSelectCommandVisitor`: Reads `tableDesc()` and computed `query.output()` attributes; delegates to `PathUtils.fromCatalogTable` once
- `OptimizedCreateHiveTableAsSelectCommandVisitor`: Same pattern as above, one call to `PathUtils.fromCatalogTable`
- `HiveCatalogTypeHandler`: Reads only Spark/Hadoop configuration strings (no HMS call); constructs `DatasetIdentifier` from in-memory config
- `V2SessionCatalogHandler`: Calls `v2Catalog.loadNamespaceMetadata(identifier.namespace())` which triggers an HMS `getDatabase` Thrift RPC

## Performance Issues Found

### Redundant `PathUtils.fromCatalogTable` Calls in `InsertIntoHiveTableVisitor` - Severity: HIGH

**Class**: `InsertIntoHiveTableVisitor`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/InsertIntoHiveTableVisitor.java:61,69,81,87,98`
**Description**: `PathUtils.fromCatalogTable` is called up to five times for the same `CatalogTable` object within a single visitor invocation. Four calls occur inside the `apply` method (two branches, each with two optional-mapped paths) and a fifth occurs in `jobNameSuffix`. Each call invokes `PathUtils.getLocationUri` -> `PathUtils.getDefaultLocationUri` -> `sparkSession.sessionState().catalog().defaultTablePath()` when the table has no explicit `storage.locationUri`.
**Root Cause**: The `DatasetIdentifier` result is never cached between the `apply` and `jobNameSuffix` calls. The optional-mapped branching structure in `apply` also duplicates the call inside each branch arm rather than computing the identifier once outside the branches.
**Code Evidence**:
```java
// apply() - overwrite branch
PathUtils.fromCatalogTable(table, context.getSparkSession().get()),  // line 61
PathUtils.fromCatalogTable(table, context.getSparkSession().get()),  // line 69 (fallback)
// apply() - non-overwrite branch
PathUtils.fromCatalogTable(table, context.getSparkSession().get()),  // line 81
PathUtils.fromCatalogTable(table, context.getSparkSession().get()),  // line 87 (fallback)
// jobNameSuffix()
PathUtils.fromCatalogTable(plan.table(), session)                    // line 98
```
**Recommendation**: Compute `DatasetIdentifier di = PathUtils.fromCatalogTable(table, session)` once at the start of `apply`, pass `di` into all branches, and store it as a local variable accessible to `jobNameSuffix` via the same local computation (or cache the result on the plan object). At minimum, compute it once before the `if (cmd.overwrite())` branch.

---

### `PathUtils.fromCatalogTable` Triggers HMS `getDatabase` RPC When Location Is Absent - Severity: HIGH

**Class**: `HiveTableRelationVisitor`, `InsertIntoHiveTableVisitor`, `CreateHiveTableAsSelectCommandVisitor`, `OptimizedCreateHiveTableAsSelectCommandVisitor`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/PathUtils.java:174-181` (`getLocationUri`)
**Description**: When `catalogTable.storage().locationUri()` is not defined (i.e., the table has no explicit location — common for managed Hive tables), `PathUtils.getDefaultLocationUri` is called, which invokes `sparkSession.sessionState().catalog().defaultTablePath(identifier)`. Inside `SessionCatalog.defaultTablePath` (Spark source line 831-835), the implementation calls `getDatabaseMetadata(db)` which calls `externalCatalog.getDatabase(db)`. In `HiveExternalCatalog`, this is implemented as `withClient { client.getDatabase(db) }` — a live synchronous Thrift RPC to the HMS with no caching layer.
**Root Cause**: `SessionCatalog.getDatabaseMetadata` has no in-process cache and calls `HiveExternalCatalog.getDatabase` directly every time. `HiveExternalCatalog` also calls `requireDbExists` first (another `databaseExists` RPC), so the actual cost is two Thrift RPCs per `defaultTablePath` call: one `databaseExists` and one `getDatabase`.
**Code Evidence**:
```java
// PathUtils.java:174-181
private static URI getLocationUri(CatalogTable catalogTable, SparkSession sparkSession) {
    if (catalogTable.storage() != null && catalogTable.storage().locationUri().isDefined()) {
        locationUri = catalogTable.storage().locationUri().get();   // fast path, in-memory
    } else {
        locationUri = getDefaultLocationUri(sparkSession, catalogTable.identifier()); // HMS RPC
    }
    return locationUri;
}

// PathUtils.java:124-125
public static URI getDefaultLocationUri(SparkSession sparkSession, TableIdentifier identifier) {
    return sparkSession.sessionState().catalog().defaultTablePath(identifier);
    // -> getDatabaseMetadata(db) -> externalCatalog.getDatabase(db) -> HMS Thrift RPC
}
```
Spark `SessionCatalog.scala:831-834`:
```scala
def defaultTablePath(tableIdent: TableIdentifier): URI = {
    val qualifiedIdent = qualifyIdentifier(tableIdent)
    val dbLocation = getDatabaseMetadata(qualifiedIdent.database.get).locationUri  // HMS call
    new Path(new Path(dbLocation), qualifiedIdent.table).toUri
}
```
**Recommendation**: Before calling `defaultTablePath`, attempt to derive the location from the warehouse configuration (which OpenLineage already reads via `PathUtils.getWarehouseLocation`) using the database and table name. This is pure string arithmetic: `<warehouse>/<database>.db/<table>`. This avoids the HMS round-trip entirely for the default-location case. Alternatively, cache the per-database `locationUri` in a session-scoped map keyed by database name.

---

### `V2SessionCatalogHandler.loadNamespaceMetadata` Triggers HMS `getDatabase` RPC - Severity: MEDIUM

**Class**: `V2SessionCatalogHandler`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/V2SessionCatalogHandler.java:41`
**Description**: `v2Catalog.loadNamespaceMetadata(identifier.namespace())` is called unconditionally on every invocation. `V2SessionCatalog.loadNamespaceMetadata` calls `catalog.getDatabaseMetadata(db).toMetadata` (Spark `V2SessionCatalog.scala:393-401`), which in turn calls `externalCatalog.getDatabase(db)` — the same HMS Thrift RPC path as the issue above. This is done even when `tableLocation` (from `properties.get(TableCatalog.PROP_LOCATION)`) is already present and the namespace location is not needed for constructing the dataset identifier.
**Root Cause**: The code fetches namespace metadata eagerly to both determine the table path (when `tableLocation` is null) and to add a symlink. The symlink addition path (`if (namespaceLocation != null)`) runs even after the table location has been resolved from `PROP_LOCATION`, meaning the HMS call is always incurred.
**Code Evidence**:
```java
// V2SessionCatalogHandler.java:41-65
Map<String, String> namespaceMetadata = v2Catalog.loadNamespaceMetadata(identifier.namespace()); // HMS RPC always
String namespaceLocation = namespaceMetadata.get("location");

String tableLocation = properties.get(TableCatalog.PROP_LOCATION);
DatasetIdentifier di;
if (tableLocation != null) {
    di = PathUtils.fromPath(new Path(tableLocation));  // table location already known
} else {
    // ... uses namespaceLocation
}

if (namespaceLocation != null) {  // symlink added even when tableLocation was sufficient
    di.withSymlink(...);
}
```
**Recommendation**: Since the symlink enrichment provides metadata lineage value, consider whether it is strictly required on the hot path. If symlinks are required, consider caching `namespaceMetadata` per `(catalogName, namespace)` pair in a session-scoped cache. If the symlink is optional for correctness, make it conditional on a configuration flag and skip the `loadNamespaceMetadata` call when `tableLocation` is already available.

---

### `CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive` Called Once Per Visitor Branch Without Caching - Severity: LOW

**Class**: `InsertIntoHiveTableVisitor`, `HiveTableRelationVisitor`, `InsertIntoHiveDirVisitor`, `CreateHiveTableAsSelectCommandVisitor`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/CatalogDatasetFacetUtils.java:18-46`
**Description**: Each visitor calls `getCatalogDatasetFacetForHive(context)` which reads `getWarehouseLocation` (reads `SparkConf` and `hadoopConfiguration`) and `getMetastoreUri` (reads `SparkConf` and `hadoopConfiguration`). While `SparkConf` is an in-memory map lookup, `hadoopConfiguration` access can involve classpath resource loading on first call. These calls repeat on every plan visitor invocation rather than being computed once at initialization.
**Root Cause**: No result is cached. These are effectively constant values for the lifetime of a SparkSession; they cannot change between plans.
**Code Evidence**:
```java
// CatalogDatasetFacetUtils.java:26-43
return PathUtils.getWarehouseLocation(sparkConf, hadoopConf)   // conf read on every call
    .map(FilesystemDatasetUtils::fromLocation)
    .map(FilesystemDatasetUtils::toLocation)
    .map(location -> Pair.of(sparkContext, location));
// ...
PathUtils.getMetastoreUri(pair.getLeft())                       // conf read on every call
    .map(PathUtils::prepareHiveUri)
    .ifPresent(uri -> builder.metadataUri(uri.toString()));
```
**Recommendation**: Compute and cache the `CatalogDatasetFacet` for Hive once per `OpenLineageContext` (e.g., as a lazily-initialized field on `OpenLineageContext` or in a companion cache keyed by `SparkContext` identity). The warehouse location and metastore URI are static for a session.

## Clean Classes

**`InsertIntoHiveDirVisitor`**: Accesses only `cmd.storage().locationUri()` (an `Option[URI]` stored directly in the plan node) and `cmd.query().schema()` (the already-resolved output schema of the subquery). Neither access touches the HMS. This class is clean.

**`HiveCatalogTypeHandler`**: Reads only from `catalogConf` (a `Map<String, String>` passed in) and `SparkConfUtils.getMetastoreUri` (reads `SparkConf` and Hadoop config, both in-memory structures). Constructs the `DatasetIdentifier` from string operations only. No HMS calls, no filesystem calls. This class is clean.

**Schema access in `HiveTableRelationVisitor`**: `x.schema()` is called via `hiveTable.tableMeta().schema()` (indirectly through `LogicalPlan.schema()` which is `output.toStructType`). The `HiveTableRelation` stores `dataCols` and `partitionCols` as `Seq[AttributeReference]` that were resolved during analysis by fetching the full `CatalogTable` from HMS. By the time the visitor runs (post-analysis), the schema is an in-memory `StructType` — no additional HMS call is made.

**Schema access in `InsertIntoHiveTableVisitor`**: `table.schema()` is a field on the `CatalogTable` case class (Spark `interface.scala:430`), populated during analysis. It is a pure in-memory read.

## Spark/Iceberg Internals Investigated

- **`spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/interface.scala`**: Confirmed that `CatalogTable` is a plain case class with `schema: StructType`, `storage: CatalogStorageFormat`, and `identifier: TableIdentifier` all stored as fields (no lazy resolution). `HiveTableRelation` stores a fully-populated `CatalogTable` as `tableMeta`, established at analysis time. `CatalogStorageFormat.locationUri` is `Option[URI]` — absent for managed tables with default locations.

- **`spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/SessionCatalog.scala`**: Confirmed that `defaultTablePath(tableIdent)` (line 831) calls `getDatabaseMetadata(db)` which calls `externalCatalog.getDatabase(db)` with no caching layer. Also confirmed that `getDatabaseMetadata` calls `requireDbExists` first (which calls `externalCatalog.databaseExists(db)`), meaning the full chain is two HMS RPCs: `databaseExists` + `getDatabase`.

- **`spark/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveExternalCatalog.scala`**: Confirmed that `getDatabase` (line 192) is wrapped in `withClient { client.getDatabase(db) }` where `withClient` holds a `synchronized` lock. The underlying `client` is `HiveClientImpl` which issues a Thrift RPC to the remote HMS. No database-level result cache exists in `HiveExternalCatalog`.

- **`spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/V2SessionCatalog.scala`**: Confirmed that `loadNamespaceMetadata(Array(db))` (line 393) calls `catalog.getDatabaseMetadata(db).toMetadata`, traversing the same HMS call chain as `defaultTablePath`.
