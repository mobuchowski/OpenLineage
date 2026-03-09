# OpenLineage Spark Integration - Performance Audit Report

## Executive Summary

The OpenLineage Spark integration is functionally correct but carries a substantial hidden performance tax on every Spark SQL execution. The core visitor infrastructure invokes uncached reflection on every plan node for every event — a 4-5x multiplier because `buildRun` fires at `SQLExecutionStart`, `SQLExecutionEnd`, and each `JobStart`/`JobEnd`. Multiple catalog handler paths (Iceberg, Delta, Hive, V2SessionCatalog, GCP Metadata) issue live network calls (REST HTTP, HMS Thrift RPC, EC2 IMDS) on every lineage event with no caching, directly exposing Spark job latency to the availability and latency of catalog services. The column-level lineage engine performs four to five independent full-tree traversals per query instead of one unified pass, and the BFS cycle-detection used during lineage resolution uses an `O(D^2)` `LinkedList.contains` visited-set that degrades severely on wide schemas. The systemic root cause across nearly all issues is the same pattern: class availability checks (`ClassLoader.loadClass`, `Class.forName`) and catalog handler lists (`CatalogUtils3.getHandlers()`) are recomputed on every event rather than being computed once at initialization.

Across 14 groups covering approximately 60 source files, **12 HIGH-severity issues** and **30+ MEDIUM-severity issues** were identified. The top three concerns are: (1) uncached network calls to remote catalogs and metadata services firing multiple times per event, (2) `CatalogUtils3.getHandlers()` reconstructing a full handler list with class-loading probes on every call (invoked 4 times per dataset per event), and (3) the column-level lineage engine performing 4 independent plan traversals per query with `O(D^2)` BFS cost in the resolution phase.

---

## Critical Issues (HIGH Severity — Must Fix)

### 1. Uncached Reflection in `isDefinedAt` Hot Path

- **Component**: Group 1 / `QueryPlanVisitor`, `AbstractQueryPlanDatasetBuilder`, `AbstractPartial`
- **Impact**: Fires 1,600–2,000 times per SQL execution across 20 non-overriding visitor subclasses. Every plan node check for every event incurs `getClass().getGenericSuperclass()` + `getActualTypeArguments()` JVM reflection calls with no caching. This is the most-called code path in the entire integration.
- **Root Cause**: The generic type argument (e.g., `InsertIntoHadoopFsRelationCommand`) is fixed at compile time and never changes between calls, but `getGenericSuperclass()` and `getActualTypeArguments()[0]` are called on every `isDefinedAt` invocation because the result is never stored. With ~20 visitors and ~20 plan nodes per query, at 4–5 events per SQL execution, this yields 1,600–2,000 reflection calls per SQL execution purely for type resolution.
- **Fix**: Cache the resolved `Class<?>` target in each visitor as `private final Class<?> targetPlanType`, set once in the constructor via a single `getGenericSuperclass()` call. The `isDefinedAt` body becomes `targetPlanType.isInstance(x)`. The same fix applies to `AbstractQueryPlanDatasetBuilder.isDefinedAtLogicalPlan` and the duplicate reflection in `QueryPlanVisitor.toString()` at line 128.

---

### 2. O(N × V) Independent Plan Traversals from `searchDependencies=true` Builders

- **Component**: Group 1 / `AbstractQueryPlanDatasetBuilder`, `OpenLineageRunEventBuilder`
- **Impact**: At least 8 builders have `searchDependencies=true` and each independently calls `qe.optimizedPlan().collect(visitor)` — a full O(N) DFS via `TreeNode.foreach`. For a typical START event with 4 active `searchDependencies=true` builders, the plan is traversed 5 times (4 builders + base `map`) with O(V) visitor checks per node, per traversal.
- **Root Cause**: There is no shared traversal or visitor multiplexing across builders. Each builder triggers its own independent `collect()` call on the optimized plan (`AbstractQueryPlanDatasetBuilder.java:72`). `OpenLineageRunEventBuilder.java:265` also issues an independent `qe.optimizedPlan().map(inputVisitor)` pass.
- **Fix**: Merge all active `searchDependencies=true` builder visitors into a single composite `PartialFunction` and call `optimizedPlan().collect(mergedVisitor)` once. Demultiplex results to the correct builder after the single traversal. This reduces 5 traversals to 1 for a typical Spark 3 input processing event.

---

### 3. `IcebergHandler` and `DeltaHandler` Call `catalog.loadTable()` on Every Event

- **Component**: Group 3, 4, 5 / `IcebergHandler`, `DeltaHandler`
- **Impact**: For Iceberg datasets, `sparkCatalog.loadTable(identifier)` is called twice per event — once in `getDatasetIdentifier()` (line 154) and once in `getDatasetVersion()` (line 244). For REST catalogs, each call is an HTTP GET to the catalog endpoint even with ETags (a network round-trip still occurs). For Delta, `catalog.loadTable()` is similarly called twice independently in `DeltaHandler.getDatasetIdentifier` (line 68) and `DeltaHandler.getDatasetVersion` (line 132) — and Spark's analysis phase already loaded this table once, making OpenLineage's calls the second and third loads of the same table per event.
- **Root Cause**: The `DataSourceV2Relation` already carries a fully resolved `table` field (a `SparkTable` or `DeltaTableV2`). The call chain strips this resolved object before reaching the handler: `DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation()` passes only `tableCatalog` and `identifier` to `CatalogUtils3.getDatasetVersion()`, discarding `relation.table()`. The handler then calls `loadTable` to re-fetch what Spark already has in memory.
- **Fix**: Pass `relation.table()` (the already-resolved `Table` object) through the version extraction path to `IcebergHandler` and `DeltaHandler`. For Iceberg, cast to `SparkTable` and call `.table().currentSnapshot()` directly. For Delta, use the `DeltaTableV2` already in `relation.table()` to access the `DeltaLog`. This eliminates all redundant `loadTable` calls from OpenLineage.

---

### 4. `CatalogUtils3.getHandlers()` Reconstructs Handler List With Class Loading on Every Call

- **Component**: Group 3, 4, 5 / `CatalogUtils3`
- **Impact**: `getHandlers(context)` is a private static method that is never cached. It instantiates 6 handler objects (`IcebergHandler`, `DeltaHandler`, `DatabricksDeltaHandler`, `DatabricksUnityV2Handler`, `JdbcHandler`, `V2SessionCatalogHandler`) and calls `hasClasses()` (which calls `ClassLoader.loadClass()` for each) on every invocation. It is called from `getDatasetIdentifier()`, `getCatalogHandler()`, `getStorageDatasetFacet()`, `getCatalogDatasetFacet()`, and `getDatasetVersion()` — meaning 4 separate handler-list reconstructions per dataset per event, each with a class-loading probe.
- **Root Cause**: The handler list result is never stored. Every public utility method on `CatalogUtils3` independently calls `getHandlers(context)` at `CatalogUtils3.java:26-36`.
- **Fix**: Cache the filtered handler list. Since class availability on the classpath cannot change at runtime, the list is stable after the first computation. Cache it as a `static volatile List<CatalogHandler>` initialized lazily with a null-check guard (context is needed for handler construction, but `hasClasses()` results are context-independent and can be cached as `static final boolean` flags independently).

---

### 5. GCP Metadata Service: Multiple Uncached HTTP Calls Per Event

- **Component**: Group 13 / `GCPUtils`, `GcpJobFacetBuilder`, `GcpRunFacetBuilder`
- **Impact**: Every Spark listener event triggers 4–8 blocking HTTP requests to `http://metadata.google.internal/computeMetadata/v1` with no caching at any level. A single job with 10 events (start/end for SQL + jobs + stages) generates approximately 80 HTTP calls to the GCP metadata service. On non-EC2/non-GCE environments (local dev, non-GCP cloud), each call blocks for up to 2 seconds per the configured timeout, adding up to 4 seconds of latency per event.
- **Root Cause**: `GCPUtils.fetchGCPMetadata()` performs a live HTTP GET on every invocation with no static fields, `volatile` flags, or memoization. Both `getDataprocRunFacetMap()` and `createDataprocOriginMap()` compound this by calling `identifyResource()` (which fetches the batch/session ID) and then immediately calling `getDataprocBatchID()` again to read the value, causing a duplicate HTTP request per call path (`GCPUtils.java:132`).
- **Fix**: Cache all metadata values in static fields, populated lazily on first access. The GCP instance metadata does not change during a job's lifetime. Use a `static volatile` field per metadata endpoint value (project ID, region, batch ID, etc.) with a one-time initialization pattern. Also refactor `identifyResource()` to return the fetched value alongside the resource type to eliminate the duplicate fetch.

---

### 6. `PathUtils.fromCatalogTable` Triggers HMS Thrift RPC for Managed Table Default Location

- **Component**: Group 9 / `PathUtils`, `HiveTableRelationVisitor`, `InsertIntoHiveTableVisitor`
- **Impact**: When a managed Hive table has no explicit `storage.locationUri` (common for all managed tables), `PathUtils.getDefaultLocationUri()` calls `sparkSession.sessionState().catalog().defaultTablePath(identifier)`, which chains into `SessionCatalog.getDatabaseMetadata(db)` → `HiveExternalCatalog.getDatabase(db)` → two synchronized HMS Thrift RPCs (`requireDbExists` + `getDatabase`). `InsertIntoHiveTableVisitor` calls `PathUtils.fromCatalogTable` up to 5 times for a single plan invocation (lines 61, 69, 81, 87, 98), multiplying the HMS cost by 5.
- **Root Cause**: `SessionCatalog.getDatabaseMetadata` has no in-process cache (`SessionCatalog.scala:831`). `HiveExternalCatalog.getDatabase` acquires a synchronized lock on the HMS client and issues a blocking Thrift RPC on every call. `InsertIntoHiveTableVisitor.apply` computes the `DatasetIdentifier` inside each branch arm rather than computing it once before the branch.
- **Fix**: (1) In `InsertIntoHiveTableVisitor`, compute `DatasetIdentifier di = PathUtils.fromCatalogTable(table, session)` once before the `if (cmd.overwrite())` branch. (2) In `PathUtils.getDefaultLocationUri`, derive the default location using pure string arithmetic from the warehouse directory and table identifier (`<warehouse>/<database>.db/<table>`) — the same formula Hive uses — rather than calling `defaultTablePath`. The warehouse path is already available via `PathUtils.getWarehouseLocation`. This eliminates all HMS RPCs for the common managed-table case.

---

### 7. Four Independent Full Plan Traversals in Column-Level Lineage Engine

- **Component**: Group 6 / `ColumnLevelLineageUtils` (spark3), `ExpressionDependencyCollector`, `InputFieldsCollector`, `OutputFieldsCollector`
- **Impact**: For every CLL-enabled query event, `buildColumnLineageDatasetFacet` triggers four separate complete DFS traversals over the logical plan tree: (1) `OutputFieldsCollector.collect`, (2) `ExpressionDependencyCollector.collect` via `plan.foreach`, (3) `InputFieldsCollector.collect`, and (4) an additional `plan.foreach` scanning for `InMemoryRelation` nodes. For cached datasets, each cached plan triggers recursive re-execution of passes 2 and 3. Each traversal is O(N) where N is the number of plan nodes.
- **Root Cause**: Each collector was developed independently with no shared traversal pass. There is no single-pass visitor dispatch that routes each plan node simultaneously to output collection, dependency collection, and input collection (`ColumnLevelLineageUtils.java:58`).
- **Fix**: Unify into a single `plan.foreach` dispatch that routes each node simultaneously to all three collectors. This reduces plan traversals from 4 to 1 (or 2 if output collection must precede dependency/input collection due to ordering constraints).

---

### 8. O(D^2) BFS Cycle Detection Using `LinkedList.contains` in `findDependentInputs`

- **Component**: Group 6, 7, 8 / `ColumnLevelLineageBuilder`
- **Impact**: `findDependentInputs` implements BFS over the expression-dependency graph using a `LinkedList<Dependency>` as the visited set. The deduplication filter calls `!dependentInputs.contains(dependency)` — an O(D) linear scan per candidate. As the BFS frontier grows over D total dependency nodes, the total cost is O(D^2). This method is called once per output column from `buildFields()`, and once per join/filter/sort/aggregate node from `datasetDependencyInputs()`. For wide schemas on complex plans: with W output columns and D total dependencies, worst-case cost is O(W × D^2).
- **Root Cause**: `LinkedList` was chosen as the visited-node store instead of `HashSet`. `Dependency` already implements correct `equals`/`hashCode` via `Objects.hash(exprId, transformationInfo)` (`ColumnLevelLineageBuilder.java:285`).
- **Fix**: Replace `dependentInputs` with `HashSet<Dependency>`. This is a one-line change that reduces the per-BFS-iteration membership test from O(D) to O(1), reducing overall BFS cost from O(D^2) to O(D).

---

### 9. `UnknownEntryFacetListener` Serializes Plan via Reflection-Heavy Jackson Pipeline on Every Event

- **Component**: Group 1 / `UnknownEntryFacetListener`, `LogicalPlanSerializer`
- **Impact**: `UnknownEntryFacetListener.build(root)` is called in `buildRunFacets` on every event where `spark_unknown` is enabled (the default). It calls `root.collectLeaves()` (full O(N) DFS) and for each unvisited leaf and root calls `planSerializer.serialize(x)`, which uses a `Proxy` object and Apache Commons `MethodUtils.invokeMethod` on each call. This runs for both START and COMPLETE events of the same SQL execution since `clear()` is called after `build()`, resetting state.
- **Root Cause**: The `spark_unknown` facet strategy serializes every unmatched leaf and root node through a reflection-heavy Jackson pipeline on every `buildRunFacets` call, regardless of whether the plan was already serialized for a prior event in the same execution (`UnknownEntryFacetListener.java:77`).
- **Fix**: (1) Gate `build()` to run at most once per query execution by caching on execution ID in `OpenLineageContext`. (2) Consider changing the default for `spark_unknown` to disabled — most production deployments gain nothing from it while paying serialization cost on every event.

---

### 10. `DeltaHandler.getDeltaTableSnapshot` Performs Uncached Reflection Lookup Per Version Call

- **Component**: Group 5 / `DeltaHandler`
- **Impact**: `getDeltaTableSnapshot` is called on every `getDatasetVersion` invocation. It uses `MethodUtils.getAccessibleMethod(deltaTable.getClass(), "snapshot")` and `MethodUtils.getAccessibleMethod(deltaTable.getClass(), "initialSnapshot")` to handle Delta API version differences. Apache Commons `MethodUtils.getAccessibleMethod` performs a fresh reflection traversal on each call (`DeltaHandler.java:155-169`). Since the Delta version in use at runtime is fixed, the method-existence check never changes outcome yet is repeated on every dataset version extraction.
- **Root Cause**: No caching of the method-existence check or the resolved `Method` objects between calls.
- **Fix**: Determine once at class-load time (or on first call via `AtomicReference`) which method name is available (`snapshot` vs. `initialSnapshot`) and store in a `static volatile String snapshotMethodName` field. Cache the resolved `Method` object. On subsequent calls, invoke directly.

---

### 11. `MergeIntoDeltaColumnLineageVisitor` Double- and Triple-Traverses Source/Target Subtrees

- **Component**: Group 11 / `MergeIntoDeltaColumnLineageVisitor`
- **Impact**: `collectInputs` explicitly calls `InputFieldsCollector.collect(context, target)` and `InputFieldsCollector.collect(context, source)` when encountering a `MergeIntoCommand` node. However, `InputFieldsCollector.collect` is already a recursive traversal, and the outer `plan.foreach` in `ExpressionDependencyCollector` already schedules visits to the same target and source subtrees. This causes the target subtree to be traversed at least twice and the source subtree at least three times (the third being from `collectExpressionDependencies` calling `ColumnLevelLineageUtils.collectInputsAndExpressionDependencies(source)` at line 72-73).
- **Root Cause**: The merge visitor incorrectly assumes it is a standalone collector rather than a callback inside an already-running `plan.foreach` tree walk driven by `ExpressionDependencyCollector.collect(MergeIntoCommand)`.
- **Fix**: Remove the explicit `InputFieldsCollector.collect(target)`, `InputFieldsCollector.collect(source)`, and `collectInputsAndExpressionDependencies(source)` calls from the merge visitor. Allow the outer recursion to handle child traversal naturally. Only merge-action-to-output mapping logic (`addDependency` calls from `getMergeActions`) needs to remain in the visitor.

---

### 12. `LogicalRelationDatasetBuilder` Makes Unconditional Filesystem `isFile()` Call and Double-Calls `relation.inputFiles()`

- **Component**: Group 2 / `LogicalRelationDatasetBuilder` (shared)
- **Impact**: `isSingleFileRelation()` calls `path.getFileSystem(hadoopConfig).isFile(path)` — a blocking HDFS/S3/GCS remote metadata call — on every lineage event for single-path relations (`LogicalRelationDatasetBuilder.java:272-283`). Additionally, `relation.inputFiles()` is called **twice** at lines 201 and 210: once for the null check and once for the `.length` access. For relations backed by `CatalogFileIndex` (typical for Hive/HMS catalog tables), `inputFiles()` triggers a fresh `listLeafFiles` scan on each call — a full recursive filesystem listing from scratch.
- **Root Cause**: The `HadoopFsRelation` already holds a `FileIndex` whose cached `leafFiles` map knows whether each path is a file or directory. This information is not consulted; instead, a new filesystem call is made each time. The double `inputFiles()` call pattern is a straightforward bug.
- **Fix**: (1) Inspect the `FileIndex` type on the relation. For `InMemoryFileIndex`, check `leafFiles.containsKey(qualifiedPath)` to classify paths without any I/O. (2) Call `relation.inputFiles()` exactly once, store the result in a local variable, then use that variable for both the null check and the `.length` access.

---

## Significant Issues (MEDIUM Severity — Should Fix)

### `hasDeltaClasses()` / `hasClasses()` / `hasKustoClasses()` / `isSnowflakeClass()` — Uncached Class Loading Across 10+ Sites

- **Component**: Groups 2, 3, 4, 5, 13, 14 / `DatasetVersionDatasetFacetUtils`, `DeltaHandler`, `AbstractDatabricksHandler`, `IcebergHandler`, `TableContentChangeDatasetBuilder`, `SqlDWDatabricksVisitor`, `SnowflakeRelationVisitor`, `SnowflakeColumnLineageVisitor`, `KustoRelationVisitor`
- **Impact**: At least 10 distinct `ClassLoader.loadClass()` / `Class.forName()` calls appear in hot-path code that fires on every plan node visit or every event with no caching. The JVM classloader caches loaded classes internally, so the overhead per call is lower than a cold load, but it still involves classloader delegation, synchronization, and a hash-table lookup. Across 4+ calls per dataset and multiple datasets per query, the aggregate cost is measurable. `KustoRelationVisitor.isDefinedAt` is called per `LogicalRelation` node.
- **Root Cause**: Universal pattern: the class-existence guard is implemented as a live `loadClass` probe rather than a `static final boolean` initialized at class-load time.
- **Fix**: Replace every `ClassLoader.loadClass(className)` / `Class.forName(className)` probe in a guard method with a `static final boolean HAS_XYZ_CLASSES` field initialized at class load time via a static initializer that performs the probe exactly once:
  ```java
  private static final boolean HAS_DELTA_CLASSES;
  static {
      boolean b = false;
      try { Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog"); b = true; }
      catch (Exception ignored) {}
      HAS_DELTA_CLASSES = b;
  }
  ```
  This pattern applies identically to `DeltaHandler.hasClasses()`, `IcebergHandler.hasClasses()`, `DatabricksDeltaHandler.hasClasses()`, `DatasetVersionDatasetFacetUtils.hasDeltaClasses()`, `SqlDWDatabricksVisitor.hasSqlDWDatabricksClasses()`, `SnowflakeRelationVisitor.isSnowflakeClass()`, `SnowflakeColumnLineageVisitor.isNotSnowflakeNode()`, and `KustoRelationVisitor.isKustoClass()`.

---

### `TableContentChangeDatasetBuilder` Allocates `new IcebergHandler(context)` 4× Per Plan-Node Check

- **Component**: Group 3 / `TableContentChangeDatasetBuilder`
- **Impact**: `isDefinedAtLogicalPlan()` creates two `new IcebergHandler(context)` instances and calls `.hasClasses()` on each (lines 170-171). When the plan is accepted, `apply()` creates two more for the same checks (lines 177-180). Each instantiation builds a 6-element `catalogTypeHandlers` list. Since `isDefinedAtLogicalPlan` is called for every node in the plan tree during traversal, this fires many times per event. Each `hasClasses()` call loads `org.apache.iceberg.catalog.Catalog`.
- **Fix**: Cache `IcebergHandler.hasClasses()` as a `static final boolean` field. Replace the 4 in-line `new IcebergHandler(context).hasClasses()` calls with a single static field reference. The `isDefinedAtLogicalPlan` hot path must not allocate handler objects.

---

### `V2SessionCatalogHandler.loadNamespaceMetadata` Triggers HMS `getDatabase` RPC on Every Dataset

- **Component**: Groups 3, 9 / `V2SessionCatalogHandler`
- **Impact**: `v2Catalog.loadNamespaceMetadata(identifier.namespace())` calls `catalog.getDatabaseMetadata(db).toMetadata` (Spark `V2SessionCatalog.scala:393`) → `externalCatalog.getDatabase(db)` — the same HMS Thrift RPC path as `defaultTablePath`. This fires on every V2 session catalog dataset resolution even when `tableLocation` from `PROP_LOCATION` is already sufficient to build the `DatasetIdentifier`. The namespace location is fetched eagerly to also add a symlink (`V2SessionCatalogHandler.java:41-65`).
- **Fix**: Cache `namespaceMetadata` per `(catalogName, namespace)` pair in a session-scoped map in `OpenLineageContext`. Namespaces rarely change during a Spark session. Alternatively, make symlink enrichment conditional on a configuration flag and skip `loadNamespaceMetadata` when `tableLocation` is already available.

---

### `IcebergHandler.getDatasetIdentifier` and `getCatalogDatasetFacet` Both Call `session.conf().getAll()`

- **Component**: Groups 3, 4 / `IcebergHandler`
- **Impact**: `SparkSession.conf().getAll()` materializes the entire Spark runtime configuration as a Scala `Map` (then converted to a Java `Map`). This is called at `IcebergHandler.java:130` (inside `getDatasetIdentifier`) and again at `IcebergHandler.java:83` (inside `getCatalogDatasetFacet`). Both methods are called for every Iceberg event. On large clusters with hundreds of config entries, this is two full config-map copies per event.
- **Fix**: Extract the catalog-specific property subset once and cache it per catalog name, either in the `IcebergHandler` instance (which is created fresh per event via `getHandlers()`) or in `OpenLineageContext`. Since Spark configuration is effectively immutable after session start, any caching of the extracted result is safe.

---

### `ExpressionTraverser.copyFor()` Allocates `new VisitorFactory()` + 18 Visitor Objects Per Recursive Step

- **Component**: Groups 6, 7, 8 / `ExpressionTraverser`, `VisitorFactory`
- **Impact**: Every call to `copyFor()` (the primary recursive mechanism in expression traversal) invokes `ExpressionTraverser.of(...)`, which always executes `new VisitorFactory()` (`ExpressionTraverser.java:84`). `VisitorFactory.expressionVisitors()` and `operatorVisitors()` then construct fresh `Arrays.asList(new AliasVisitor(), new CaseWhenVisitor(), ...)` lists with 6 and 12 new objects respectively. For a complex expression tree with depth D and branching factor B, this results in O(B^D) allocations of VisitorFactory + visitor lists, generating heavy GC pressure on queries with deeply nested expressions (CASE WHEN chains, nested COALESCE, complex aggregations).
- **Fix**: Pass `this.visitorFactory` through `copyFor()` instead of calling `ExpressionTraverser.of()`. Since all visitors are stateless, a single shared `VisitorFactory` instance per traversal root is correct. Alternatively, make `VisitorFactory.expressionVisitors()` and `operatorVisitors()` return `static final` immutable lists.

---

### `PlanUtils.safeIsInstanceOf` Calls `Class.forName()` With Inverted Logic Bug

- **Component**: Group 1 / `PlanUtils`, `CreateReplaceDatasetBuilder`
- **Impact**: `PlanUtils.safeIsInstanceOf(Object instance, String classCanonicalName)` calls `Class.forName(classCanonicalName)` on every invocation without caching. `CreateReplaceDatasetBuilder` calls this from `isDefinedAtLogicalPlan()` (triggered on every plan node). Additionally the implementation has an inverted `isAssignableFrom` logic bug: line 197-198 uses `instance.getClass().isAssignableFrom(c)` which checks if the instance's class is a *supertype* of `c` — the opposite of the intended "is instance an instance of that class" check. This means the guard silently never matches its intended check.
- **Fix**: (1) Fix the logic to `c.isInstance(instance)`. (2) In `CreateReplaceDatasetBuilder`, resolve the `Class` once in a static initializer as `private static final Class<?> CREATE_V2_TABLE_CLASS` with null fallback if absent; use `CREATE_V2_TABLE_CLASS != null && CREATE_V2_TABLE_CLASS.isInstance(x)` in `isDefinedAtLogicalPlan`. Remove `Class.forName()` from the traversal hot path entirely.

---

### `catalogTableFor` Issues Synchronous HMS Call on Each Matching Plan Node (7 Visitor Classes)

- **Component**: Group 1 / `QueryPlanVisitor`
- **Impact**: `catalogTableFor(TableIdentifier tableId)` calls `session.sessionState().catalog().getTableMetadata(tableId)` synchronously from `apply()`. In Hive Metastore environments this is a blocking Thrift RPC. Seven visitor classes call this unconditionally on match: `AlterTableAddColumnsCommandVisitor`, `AlterTableRenameCommandVisitor`, `AlterTableSetLocationCommandVisitor`, `AlterTableAddPartitionCommandVisitor`, `DropTableCommandVisitor`, `TruncateTableCommandVisitor`, `LoadDataCommandVisitor` (`QueryPlanVisitor.java:77-88`). For DDL-heavy workloads where `buildRun` fires multiple times per execution (START + COMPLETE), the same table is fetched repeatedly.
- **Fix**: Add a `Map<TableIdentifier, Optional<CatalogTable>>` cache within `OpenLineageContext` that persists for the lifetime of a single SQL execution context. Check the cache before calling `getTableMetadata`. This eliminates duplicate Thrift calls across multiple events for the same table within one execution.

---

### `SparkPropertyFacetBuilder` Scans All Spark Config Entries on Every Event With Exception-as-Control-Flow

- **Component**: Group 12 / `SparkPropertyFacetBuilder`
- **Impact**: `buildFacet()` calls `conf.getAll()`, which copies all settings from an internal `ConcurrentHashMap` to a new array on each invocation — hundreds of entries in production. This builder fires on every `SparkListenerEvent`. Additionally `session.conf().get(item)` throws `NoSuchElementException` for missing keys (which is caught silently at line 76), incurring exception-construction overhead per absent property per event.
- **Fix**: Replace `conf.getAll()` with direct per-key lookups for only the allowed properties: `allowedProperties.forEach(key -> conf.getOption(key).foreach(v -> m.put(key, v)))`. Use `session.conf().getOption(item)` to avoid exception-as-control-flow for missing keys.

---

### `DatabricksEnvironmentFacetBuilder` Makes Synchronous DBFS API Call for Mount Points on Every Job Start

- **Component**: Group 12 / `DatabricksEnvironmentFacetBuilder`
- **Impact**: `getDatabricksMountpoints()` reflectively calls `DbfsUtils.mounts()`, which enumerates all DBFS mount points via a synchronous call to the Databricks DBFS service. This runs on the Spark event bus thread during `SparkListenerJobStart` processing. On workspaces with many mounts (dozens to hundreds), this blocks the event bus for a non-deterministic duration. No caching is applied; the call repeats on every job start (`DatabricksEnvironmentFacetBuilder.java:116-155`).
- **Fix**: Cache the mount point list after the first call (mount points change rarely during a running job). Add a configurable timeout. Consider making mount-point collection opt-in via a configuration flag defaulting to disabled.

---

### `DebugRunFacetBuilderDelegate.scanLogicalPlan()` Has O(N^2) Plan String Generation

- **Component**: Group 12 / `DebugRunFacetBuilderDelegate`
- **Impact**: `scanLogicalPlan()` recursively walks every node in the logical plan tree and stores `node.toString()` as the `desc` field. `LogicalPlan.toString()` delegates to Spark's `treeString()`, which itself recursively walks the entire subtree rooted at that node. For a plan with N nodes, this produces N full subtree text serializations — O(N^2) total string work. This facet is disabled by default but the regression is severe if enabled on complex queries.
- **Fix**: Replace `node.toString()` with `node.simpleString(SQLConf.get.maxToStringFields)` or `node.nodeName()`, which generates only a single-line description of the node without recursing into children. This reduces complexity from O(N^2) to O(N).

---

### `MergeIntoDeltaColumnLineageVisitor` Has O(N×M) Linear Scans in `getOutputExprIdByFieldName`

- **Component**: Group 11 / `MergeIntoDeltaColumnLineageVisitor`, `MergeIntoCommandEdgeColumnLineageBuilder`, `ColumnLevelLineageBuilder`
- **Impact**: `getOutputExprIdByFieldName(String field)` scans the entire `outputs.keySet()` linearly (O(M) where M is the schema field count) on every call. In `collectExpressionDependencies`, it is called twice per merge action — once in the filter and once in the `forEach`. With N merge actions and M output columns, total cost is O(N × M) per MERGE statement (`MergeIntoDeltaColumnLineageVisitor.java:43,83,92`). `MergeIntoCommandEdgeColumnLineageBuilder.java:67,103,112` has the same pattern.
- **Fix**: Add a secondary `Map<String, ExprId> outputsByName` field to `ColumnLevelLineageBuilder`, populated on each `addOutput` call. Replace `getOutputExprIdByFieldName` with an O(1) `outputsByName.get(field)` lookup. Also change `mergeActionsExprIds` from `Collectors.toList()` to `Collectors.toSet()` to make the subsequent `!contains(id)` filter O(1) instead of O(N).

---

### `AbstractDatabricksHandler.getDatasetIdentifier` Uses Uncached `MethodUtils.invokeMethod` Per Dataset

- **Component**: Group 5 / `AbstractDatabricksHandler`
- **Impact**: `MethodUtils.invokeMethod(tableCatalog, true, "isPathIdentifier", identifier)` is called on every dataset identifier resolution. Apache Commons `MethodUtils.invokeMethod` performs a fresh method lookup (traversing the class hierarchy) on every call and does not cache the resolved `Method`. This fires for every Databricks table event (`AbstractDatabricksHandler.java:76-80`).
- **Fix**: Cache the `Method` object for `isPathIdentifier` after the first successful lookup in a `static volatile Method` field. Invoke the cached `Method` directly on subsequent calls.

---

### `ColumnLevelLineageBuilder.addOutput()` Performs Linear Schema Scan on Every Output Field Registration

- **Component**: Groups 6, 7 / `ColumnLevelLineageBuilder`
- **Impact**: `addOutput(ExprId, String)` performs `schema.getFields().stream().filter(field -> field.getName().equals(attributeName)).findAny()` — an O(S) linear scan — on every output attribute registration (`ColumnLevelLineageBuilder.java:92`). `getOutputExprIdByFieldName()` and `getInputsUsedFor(String)` at lines 139 and 258 repeat the same O(S) pattern. Called for every output attribute of every plan node, with a schema of S fields, this is O(P × S) for a plan with P projection nodes.
- **Fix**: Build a `Map<String, SchemaDatasetFacetFields>` index from `schema.getFields()` once in the constructor. Use O(1) map lookups in `addOutput()`, `getOutputExprIdByFieldName()`, and `getInputsUsedFor(String)`. Note: resolve the case-sensitivity inconsistency — `addOutput` uses case-sensitive comparison while `getInputsUsedFor` uses `equalsIgnoreCase`.

---

### `MergeRowsColumnLineageVisitor` Re-Materializes Scala-to-Java Lists O(Columns × Instructions) Times

- **Component**: Group 11 / `MergeRowsColumnLineageVisitor`
- **Impact**: `ScalaConversionUtils.fromSeq(instruction.outputs())` is called inside the inner body of a nested loop: outer loop over C output column positions, inner loop over I instructions. This materializes a new Java `List` by copying the same Scala `Seq` for each `(column, instruction)` pair, producing C redundant copies of the same list per instruction — O(C × I) total `fromSeq` calls (`MergeRowsColumnLineageVisitor.java:58-76`).
- **Fix**: Restructure so instructions are the outer loop and column positions are inner. Pre-materialize `instruction.outputs()` once per instruction before the inner position loop.

---

### `CoalesceVisitor` Double-Traverses Every Child Sub-Tree

- **Component**: Group 7 / `CoalesceVisitor`
- **Impact**: For `COALESCE(e1, e2, ..., eN)`, each child expression `e_i` is traversed twice: once as `indirect/conditional` and once as `direct` via two separate `copyFor(e).traverse()` calls (`CoalesceVisitor.java:36-41`). For a `COALESCE` with N children, this means 2N full sub-tree traversals. For nested COALESCE expressions (common in ETL null-chain patterns), the work grows multiplicatively across levels.
- **Fix**: Collect the set of `ExprId` leaf nodes discovered during a single traversal and register both dependency types against those IDs, avoiding the second full descent. Alternatively, register only the indirect edge at the `COALESCE` level (not via sub-tree traversal) and perform a single direct traversal.

---

### `AggregateExpressionVisitor` Performs Uncached Reflection Lookup Per Aggregate Expression

- **Component**: Groups 7, 8 / `AggregateExpressionVisitor`
- **Impact**: On every call to `apply()`, `MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId")` is called to detect whether this is standard Spark or Databricks runtime (`AggregateExpressionVisitor.java:40`). This check is purely environmental — the answer is fixed at JVM startup — yet it runs for every `AggregateExpression` node in every expression tree. On plans with many aggregates (50+), this is 50 reflective class introspections per event.
- **Fix**: Cache the boolean result as `private static final boolean HAS_RESULT_ID = (MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId") != null)`. Use `if (HAS_RESULT_ID)` in `apply()`. Cache the Databricks-path `Method` handle as a `static final` to avoid per-call reflective dispatch.

---

### `newHadoopConfWithOptions` Creates Full Deep Copy of Hadoop Configuration Per Event

- **Component**: Group 2 / `LogicalRelationDatasetBuilder` (shared)
- **Impact**: `session.sessionState().newHadoopConfWithOptions(relation.options())` is called on every `HadoopFsRelation` event (`LogicalRelationDatasetBuilder.java:186-187`). This deep-copies the session's Hadoop `Configuration` object (cloning all internal hash maps, hundreds of keys in typical clusters) and then overlays the relation's options. This fires just to obtain a `FileSystem` instance for path resolution — work that can be eliminated by inspecting the already-computed `FileIndex` on the relation.
- **Fix**: Access the `PartitioningAwareFileIndex.hadoopConf` already embedded in the relation's `FileIndex`, or use `sparkContext.hadoopConfiguration` directly and apply only the necessary options. With the `isSingleFileRelation` filesystem call eliminated (see HIGH issue #12), the hadoop config copy on the single-file branch is no longer needed at all.

---

### Kafka/Streaming: Uncached `getDeclaredField` and `setAccessible` on Every Event

- **Component**: Group 10 / `KafkaRelationVisitor`, `KinesisMicroBatchStreamStrategy`
- **Impact**: `KafkaRelationVisitor.apply()` performs `relation.getClass().getDeclaredField("sourceOptions")` + `setAccessible(true)` on every invocation (lines 186-188). `getDeclaredField` involves a linear scan of the class's declared fields and clones the result; `setAccessible(true)` in Java 9+ triggers security checks. `KinesisMicroBatchStreamStrategy` similarly uses two-level `FieldUtils.readField` per event. `KinesisMicroBatchStreamStrategy.java:25` also allocates `new HostListNamespaceResolverConfig()` whose result is immediately discarded (dead-code garbage).
- **Fix**: Cache the `Field` object for `sourceOptions` as `private static volatile Field SOURCE_OPTIONS_FIELD` with lazy initialization. Remove the dead-code `new HostListNamespaceResolverConfig()` line from the Kinesis constructor. Standardize `KinesisMicroBatchStreamStrategy` to use `commons-lang3` `FieldUtils` (it currently imports the deprecated `commons-lang 2.x` variant).

---

### `DistinctVisitor` and `CreateTableAsSelectVisitor` Double-Visit Child Subtrees

- **Component**: Group 8 / `DistinctVisitor`, `CreateTableAsSelectVisitor`
- **Impact**: Both visitors call `collectFromOperator(builder, child)` on a child `LogicalPlan` node while the driving `plan.foreach` in `ExpressionDependencyCollector` will also visit that same child in normal pre-order traversal order. This causes all dependencies registered from the child subtree to be added to the builder a second time (`DistinctVisitor.java:23`, `CreateTableAsSelectVisitor.java:24`).
- **Fix**: For `DistinctVisitor`, directly map the child's output attributes to the parent's output attributes (identity mapping) rather than re-running full dependency collection. For `CreateTableAsSelectVisitor`, document clearly why the `children().isEmpty()` guard is required, or replace with a direct attribute walk that does not recurse.

---

### `IcebergMergeIntoVisitor.isDefinedAt` Allocates a List and Calls `getCanonicalName` Per Plan Node

- **Component**: Group 8 / `IcebergMergeIntoVisitor`
- **Impact**: `isDefinedAt` allocates a new `Arrays.asList(...)` wrapper and calls `operator.getClass().getCanonicalName()` on every plan node during the full `plan.foreach` traversal (`IcebergMergeIntoVisitor.java:40-43`). This fires for every node in the plan tree for every CLL event.
- **Fix**: Cache the two `Class` objects (`MergeInto`, `MergeRows`) as `static final` fields resolved at class-load time using `Class.forName` with null fallback. Replace the `isDefinedAt` body with two `Class.isInstance(operator)` reference comparisons — zero allocation, zero string construction.

---

### `SnowflakeColumnLineageVisitor.isNotSnowflakeNode` Emits `log.debug()` Per Plan Node

- **Component**: Group 13 / `SnowflakeColumnLineageVisitor`
- **Impact**: `isNotSnowflakeNode()` calls `log.debug(...)` twice per plan node visited, even when debug logging is disabled at the SLF4J level. SLF4J logger calls with string concatenation or unguarded format arguments still perform argument resolution before the logger decides to discard the message (`SnowflakeColumnLineageVisitor.java:147-151`). Combined with the uncached `loadClass` call on the same path, this adds unnecessary overhead per node during plan traversal.
- **Fix**: Wrap all debug log calls in `if (log.isDebugEnabled())` guards in the hot path. Cache the loaded `Class<?>` as a `static` field.

---

## Minor Issues (LOW Severity — Nice to Fix)

### Misplaced Micrometer Timer Records Function Object Creation, Not Plan Traversal
- **Component**: Group 1 / `OpenLineageRunEventBuilder`
- The timer at `OpenLineageRunEventBuilder.java:308-325` wraps only the `Function1` construction inside `visitLogicalPlan()`, not the actual plan traversal (`qe.optimizedPlan().map(inputVisitor)`), which happens after the lambda returns. The timer records near-zero time and provides no useful signal. Move the timer to wrap the full `buildInputDatasets` body.

### `InputFieldsCollector.isAssignableFrom` Arguments Transposed — Dead Code Branch
- **Component**: Group 6 / `InputFieldsCollector`
- `(plan.getClass()).isAssignableFrom(UnaryNode.class)` is always `false` for concrete plan nodes (`InputFieldsCollector.java:59`). The `UnaryNode` optimized path is dead code. Correct to `UnaryNode.class.isAssignableFrom(plan.getClass())`.

### `ExpressionTraverser.isMasking` Linear-Scans a Static `List<Class>` Per Expression Node
- **Component**: Groups 6, 8 / `ExpressionTraverser`
- `isMasking(Expression)` scans a `List<Class>` with `stream().anyMatch()` and separately scans `classNames` as a `List<String>` (`ExpressionTraverser.java:48-51`). Both are called for every fallback expression node. Replace `classes` with `Set<Class<?>>` for O(1) `contains` lookup. Replace the `classNames` single-element list with a direct equality check.

### `UnionVisitor` Uses `LinkedList` as Intermediate Collection Then Accesses by Index
- **Component**: Group 8 / `UnionVisitor`
- `childrenAttributes` is declared as `List<ArrayList<Attribute>>` but instantiated as a `LinkedList` (`UnionVisitor.java:44`). Indexed access via `childrenAttributes.get(childIndex)` inside an `IntStream.range` lambda is O(childIndex) per access. Change to `ArrayList`.

### `FileScanRDDExtractor` Iterates All Files Across All Partitions For Deduplication Done Later
- **Component**: Group 14 / `RddDatasetInfoExtractor.FileScanRDDExtractor`
- `FileScanRDDExtractor.extract` iterates all `PartitionedFile` objects across all `FilePartition`s (`RddDatasetInfoExtractor.java:168-181`) to extract parent paths, with deduplication deferred to `PlanUtils.findDatasetIdentifiers`. For tables with thousands of partitions, this is O(total_file_count). Short-circuit by examining only the first file of each `FilePartition` — files within a partition are co-located and share the same root directory.

### `DataSourceRDDExtractor` Bypasses Spark's RDD Partition Cache
- **Component**: Group 14 / `RddDatasetInfoExtractor.DataSourceRDDExtractor`
- Calls `rdd.getPartitions()` (protected Scala method, uncached) instead of `rdd.partitions()` (final Scala method with `@volatile` caching), causing `DataSourceRDDPartition` objects to be reallocated on every call (`RddDatasetInfoExtractor.java:284-296`).

### `KinesisMicroBatchStreamStrategy` Dead-Code Object Allocation in Constructor
- **Component**: Group 10 / `KinesisMicroBatchStreamStrategy`
- `new HostListNamespaceResolverConfig()` at line 25 is constructed and immediately discarded. Remove this line.

### `CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive` Reads Hadoop Config Per Visit
- **Component**: Group 9 / `CatalogDatasetFacetUtils`
- `getWarehouseLocation` and `getMetastoreUri` read from `SparkConf`/`hadoopConfiguration` on every visitor invocation. Cache these values once in `OpenLineageContext` as they are constant for a session.

### `hasSqlDWDatabricksClasses()` Called Twice Per Query for Visitor List Construction
- **Component**: Group 5 / `SqlDWDatabricksVisitor`
- Called from `BaseVisitorFactory.getBaseCommonVisitors` which is called for both input and output visitor list construction — twice per query. Cache as `static final boolean`.

### `TopicPartitionProxy` Uses Unnecessary Reflection When `TopicPartition` Is Directly Importable
- **Component**: Group 10 / `TopicPartitionProxy`, `KafkaMicroBatchStreamStrategy`
- `MethodUtils.invokeMethod(topicPartition, "topic")` via the proxy performs a reflective method lookup per topic partition. Since `org.apache.kafka.common.TopicPartition` is directly imported at line 21 of `KafkaMicroBatchStreamStrategy`, replace the proxy call with `((TopicPartition) item).topic()`.

### `DebugRunFacetBuilder` Fires on Every Event When Enabled
- **Component**: Group 12 / `DebugRunFacetBuilder`
- `isDefinedAt()` returns `true` for every `Object` (no type filtering) when debug is enabled, causing the expensive `scanLogicalPlan` + classpath scan + metrics collection to run on every `SparkListenerEvent`. Restrict to `SparkListenerJobEnd` or `SparkListenerSQLExecutionEnd`.

### `DebugRunFacetSerializer` Double-Serializes the Debug Facet for Size Check
- **Component**: Group 12 / `DebugRunFacetSerializer`
- Fully serializes the debug facet to a `String` to measure byte size, then serializes again when emitting (`DebugRunFacetSerializer.java:41-46`). Use a `CountingOutputStream` with a null sink for the size check, then write the already-serialized `byte[]` directly via `writeRawValue()`.

### `LogicalPlanRunFacetBuilder` Should Enforce Size Limit at `build()` Time
- **Component**: Group 12 / `LogicalPlanRunFacetBuilder`, `LogicalPlanFacet`
- `LogicalPlanFacet.getPlan()` calls `plan.toJSON()` at Jackson serialization time (`LogicalPlanFacet.java:28`), too late to apply size checks before event emission. Serialize at `build()` time and apply a byte-size or node-count limit. This facet is disabled by default; add a prominent documentation warning about payload size.

### `AbstractDatabricksHandler.getDatasetIdentifier` Falls Back to `loadTable` When Location Is Absent
- **Component**: Group 3 / `AbstractDatabricksHandler`
- When neither path identifier nor the `location` property is present, the handler calls `tableCatalog.loadTable(identifier)` to retrieve table properties from the Databricks catalog (`AbstractDatabricksHandler.java:88-98`). The `relation.table()` object is already available in the caller with the same properties. Pass the resolved `Table` through to avoid this secondary `loadTable`.

---

## Top 5 Performance Bottlenecks

Ranked by expected real-world impact on Spark job wall-time and Spark event processing latency:

1. **Remote Catalog Calls (Iceberg `loadTable`, HMS Thrift, GCP IMDS) Firing on Every Lineage Event** — This is the dominant real-world bottleneck. A single Iceberg dataset triggers 2 `loadTable` HTTP GETs per event, and with 4-5 events per SQL execution, a 10-table query generates 80-100 HTTP requests to the Iceberg catalog per execution. For HMS-backed tables, each event may trigger 2-3 synchronized Thrift RPCs. For GCP Dataproc, each event generates 4-8 HTTP requests to the metadata service. These calls add directly to driver wall-time and block the Spark event bus. The fix (pass already-resolved `relation.table()` through the call chain and cache results) can reduce remote calls from O(events × tables) to O(1) per session.

2. **`CatalogUtils3.getHandlers()` Reconstruction + Class Loading Called 4× Per Dataset Per Event** — This is the most systemic architectural issue. Every public method on `CatalogUtils3` independently rebuilds the entire handler list, each time allocating 6 handler objects and calling `ClassLoader.loadClass` for 4 of them. For a query with 20 input tables, this is 80 handler list rebuilds and 320 class-load probes per event. The fix (a single cached handler list) is a one-time change that eliminates this overhead entirely.

3. **Multiple Independent Plan Traversals (Core + CLL)** — The visitor infrastructure performs 4-5 independent `O(N)` plan traversals per event (from `searchDependencies=true` builders alone), and the column-level lineage engine adds 4 more independent traversals per CLL-enabled query. For complex plans with 50+ nodes, this is 400+ node visits per event from traversal overhead alone, before any lineage logic runs. Unifying to a single traversal pass per phase is an architectural change that would cut traversal overhead by 80%.

4. **O(D^2) BFS in `ColumnLevelLineageBuilder.findDependentInputs`** — For wide schemas (100+ columns) with complex join-heavy plans, the `LinkedList.contains` visited-set in BFS resolution produces O(W × D^2) cost at CLL build time. This is a single-line fix (`LinkedList` → `HashSet`) with potentially large impact on queries that have many transitive expression dependencies (multi-way joins, complex CASE WHEN chains, deeply nested subqueries).

5. **`ExpressionTraverser.copyFor()` Allocating `new VisitorFactory()` + 18 Visitor Objects Per Expression Node** — For queries with complex expression trees (nested COALESCE/CASE WHEN chains, many aggregate expressions), the `new VisitorFactory()` inside every `copyFor()` call generates substantial GC pressure. On a 100-expression tree with depth 5, this creates hundreds of short-lived `VisitorFactory` + visitor objects per query event. The fix (pass `this.visitorFactory` through `copyFor`) is a 3-line change with zero semantic impact.

---

## Recommendations by Category

### Eliminate Hidden Network/IO Calls

**Iceberg catalog (`IcebergHandler.java:154, :244`)**: Pass `relation.table()` from `DataSourceV2Relation` through `DatasetVersionDatasetFacetUtils.extractVersionFromDataSourceV2Relation()` to `CatalogUtils3.getDatasetVersion()` and `IcebergHandler`. Use `((SparkTable) relation.table()).table().currentSnapshot()` instead of calling `loadTable` again. Eliminates 2 HTTP calls per Iceberg dataset per event.

**Delta catalog (`DeltaHandler.java:68, :132`)**: Pass the already-resolved `DeltaTableV2` from `relation.table()` to `DeltaHandler.getDatasetIdentifier` and `getDatasetVersion`. Extract the snapshot from the `DeltaLog` already loaded in `DeltaTableV2`. Eliminates 2 `DeltaCatalog.loadTable` calls per event.

**Hive Metastore in `PathUtils` (`PathUtils.java:124-125`)**: Replace `sparkSession.sessionState().catalog().defaultTablePath(identifier)` with string arithmetic: `<warehouse>/<database>.db/<table>`. The warehouse location is already read by `PathUtils.getWarehouseLocation`. Eliminates 2 HMS RPCs (`requireDbExists` + `getDatabase`) per managed table reference.

**Hive Metastore in `V2SessionCatalogHandler` (`V2SessionCatalogHandler.java:41`)**: Cache `namespaceMetadata` per namespace in `OpenLineageContext`. Make symlink enrichment conditional on a config flag.

**GCP Metadata Service (`GCPUtils.java:240`)**: Cache all `fetchGCPMetadata()` results in `static volatile` fields. The GCP instance metadata is immutable for the lifetime of a job. This single change reduces ~80 HTTP calls per 10-event job to 4-8 (one set of initialization calls).

**`LogicalRelationDatasetBuilder.isSingleFileRelation` (`LogicalRelationDatasetBuilder.java:272-283`)**: Replace `path.getFileSystem(hadoopConfig).isFile(path)` with an inspection of the `FileIndex.leafFiles` cache already held by the relation.

**`HadoopRDDExtractor` / `NewHadoopRDDExtractor` (`PlanUtils.java:280`)**: Add a configuration flag to skip `getFileStatus` per path. For typical `HadoopRDD` cases, all paths are directories.

---

### Fix Plan-Size Scaling (O(N) and Worse)

**Unify `searchDependencies=true` traversals** (`AbstractQueryPlanDatasetBuilder.java:72`, `OpenLineageRunEventBuilder.java:265`): Merge all active builder visitors into a single composite `PartialFunction` and call `optimizedPlan().collect(mergedVisitor)` once. Demultiplex results after the single traversal. Reduces 5 traversals to 1 per event.

**Unify CLL traversals** (`ColumnLevelLineageUtils.java:58`): Combine `OutputFieldsCollector`, `ExpressionDependencyCollector`, and `InputFieldsCollector` into a single `plan.foreach` dispatch. Reduces 4 traversals to 1-2 per CLL-enabled event.

**Fix BFS visited-set** (`ColumnLevelLineageBuilder.java:285`): Change `new LinkedList<>()` to `new HashSet<>()`. One line. Reduces BFS cost from O(D^2) to O(D).

**Fix `getOutputExprIdByFieldName` index** (`ColumnLevelLineageBuilder.java:92`): Build `Map<String, SchemaDatasetFacetFields>` at construction time. Reduces O(S) scan to O(1) lookup in `addOutput`, `getOutputExprIdByFieldName`, and `getInputsUsedFor`.

**Fix `MergeIntoDeltaColumnLineageVisitor` duplicate traversals** (`MergeIntoDeltaColumnLineageVisitor.java:31-73`): Remove explicit `InputFieldsCollector.collect(target/source)` and `collectInputsAndExpressionDependencies(source)` calls. Let the outer `plan.foreach` handle these subtrees.

**Fix `DebugRunFacetBuilderDelegate.scanLogicalPlan`** (`DebugRunFacetBuilderDelegate.java:176`): Replace `node.toString()` with `node.simpleString(...)`. Reduces O(N^2) to O(N).

**Fix `UnionVisitor` linked-list index access** (`UnionVisitor.java:44`): Change `new LinkedList<>()` to `new ArrayList<>()`.

---

### Caching and Memoization Opportunities

**`CatalogUtils3.getHandlers()` (most impactful)** (`CatalogUtils3.java:26-36`): Add `private static volatile List<CatalogHandler> CACHED_HANDLERS = null`. Initialize once with a double-checked lock on first call. The classpath is immutable; this cache is valid for the JVM lifetime. This eliminates 4 × (6 object allocations + 4 `loadClass` calls) per dataset per event.

**All `hasXyzClasses()` / `isXyzClass()` guards** (10+ locations across Groups 2, 3, 4, 5, 13, 14): Convert every `ClassLoader.loadClass(name)` guard to a `static final boolean HAS_XYZ = probe()` field. This is the single most-widespread pattern in the codebase and every instance is a straightforward one-time fix.

**`IcebergHandler` catalog properties** (`IcebergHandler.java:83, :130`): Cache the result of `getCatalogProperties(conf.getAll(), catalogName)` per catalog name in `OpenLineageContext`. Both `getCatalogDatasetFacet` and `getDatasetIdentifier` call this independently.

**`QueryPlanVisitor` type argument** (`QueryPlanVisitor.java:97`): Cache `(Class<?>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]` as `private final Class<?> targetPlanType` in the constructor. 20 visitor subclasses benefit.

**`DeltaHandler.getDeltaTableSnapshot` version probe** (`DeltaHandler.java:155`): Cache the result of `MethodUtils.getAccessibleMethod(DeltaTableV2.class, "snapshot") != null` as a `static volatile boolean`. One-time reflection at first call.

**`catalogTableFor` HMS lookup** (`QueryPlanVisitor.java:83`): Add an execution-scoped `Map<TableIdentifier, Optional<CatalogTable>>` in `OpenLineageContext`. Deduplicate HMS calls for the same table across START and COMPLETE events.

**GCP metadata** (`GCPUtils.java:240`): `static volatile` fields per endpoint, populated once on first invocation.

**`CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive`** (`CatalogDatasetFacetUtils.java:26-43`): Cache warehouse location and metastore URI once per `OpenLineageContext`.

**`AggregateExpressionVisitor` version check** (`AggregateExpressionVisitor.java:40`): `static final boolean HAS_RESULT_ID`.

**`AbstractDatabricksHandler.isPathIdentifier` method** (`AbstractDatabricksHandler.java:76`): `static volatile Method IS_PATH_IDENTIFIER_METHOD`.

---

### Architecture-Level Changes

**Single-pass plan traversal**: The most impactful architectural change is to refactor both the core visitor infrastructure (Groups 1-5) and the CLL engine (Groups 6-8) to perform a single unified `plan.foreach` per processing phase. For the core visitor: merge all `searchDependencies=true` builders into a multiplexed composite visitor. For CLL: unify `OutputFieldsCollector`, `ExpressionDependencyCollector`, and `InputFieldsCollector` into one `plan.foreach` callback that routes each node to all three collectors simultaneously. This yields a minimum 4× reduction in plan traversal work per event.

**Thread the resolved `Table` object through catalog handlers**: The `CatalogHandler` interface should be extended with overloads that accept a pre-resolved `Table` object alongside `TableCatalog` and `Identifier`. Callers that have `relation.table()` (all `DataSourceV2Relation` paths) should use these overloads. Handlers fall back to `loadTable` only when no pre-resolved table is available (e.g., DDL commands). This is a breaking API change for the `CatalogHandler` interface but eliminates all redundant catalog round-trips.

**`VisitorFactory` as a static singleton**: `VisitorFactory.operatorVisitors()` and `expressionVisitors()` return new object lists on every call. Make both return `static final` immutable lists. All 18 visitor classes (12 operator, 6 expression) are stateless and safe to share. This eliminates `O(B^D)` allocations during expression traversal.

**Event-scoped context cache**: Introduce a lightweight `EventProcessingContext` object attached to each `buildRun` invocation (and cleared at its end) that holds: the resolved `Table` per `(catalog, identifier)`, the `CatalogTable` per `TableIdentifier`, and the namespace metadata per `(catalog, namespace)`. This is the cleanest architectural solution for the "re-fetch what Spark already computed" class of problems, and avoids the need for ad-hoc static caches that can cause memory leaks or stale data.

**Unified class-availability registry**: Introduce a `PluginClassAvailabilityRegistry` singleton populated at `ContextFactory` initialization that performs all `ClassLoader.loadClass` probes once and exposes `static boolean IS_DELTA_AVAILABLE`, `IS_ICEBERG_AVAILABLE`, etc. All `hasClasses()` and `isDefinedAt`-guard implementations delegate to this registry. This eliminates the 10+ independent probe sites and ensures each probe runs exactly once per JVM.

---

## Clean Components

The following components have no significant performance issues and can serve as reference implementations:

- **`AbstractQueryPlanInputDatasetBuilder` and `AbstractQueryPlanOutputDatasetBuilder`**: Clean wrappers with direct `isDefinedAt(SparkListenerEvent)` checks — no reflection in the event-dispatch path.
- **`InternalEventHandlerFactory`**: `ServiceLoader.load()` and visitor construction happen once at `ContextFactory` initialization. The `generate()` helper is a clean flatMap. No repeated I/O on the hot path.
- **`BaseVisitorFactory`**: Class availability checks (`hasHiveClasses()`, `hasKafkaClasses()`) are evaluated once per SQL execution context creation, not per event.
- **`OpenLineageRunEventTimeoutExecutor`**: Correctly uses a shared `ExecutorService` via `getOrCreateExecutor()` avoiding repeated thread pool creation.
- **`InsertIntoHiveDirVisitor`**: Accesses only `cmd.storage().locationUri()` and `cmd.query().schema()` — both in-memory, no HMS calls.
- **`HiveCatalogTypeHandler`**: Reads only from config maps and `SparkConfUtils` — no HMS or filesystem calls.
- **`KafkaBootstrapServerResolver`**: Pure string manipulation, zero I/O, zero network.
- **`TransformationInfo`**: Uses pre-allocated static singletons for all common transformation types. The `merge()` method is O(1) with no allocation on the common paths.
- **`ColumnLevelLineageBuilder.addDependency()`**: Correctly interns `Dependency` objects via `commonDependencies` `HashMap`, reducing allocation for repeated dependency edges.
- **`CommitReportsFacetBuilder`, `ScanReportsFacetBuilder`, `IcebergCommitReportOutputDatasetFacetBuilder`**: Pure data transformation from already-materialized in-memory POJOs. No I/O, no reflection.
- **`CosmosHandler`**: `hasClasses()` is called only once at class-loading time via `CatalogUtils3`'s static field. Identifier extraction is pure string parsing.
- **`JoinVisitor`**: Does NOT cross-product output attributes of join sides. Correctly traverses only the join condition expression, making its complexity O(depth of condition expression tree).
- **`HadoopCatalogTypeHandler`, `RestCatalogTypeHandler`, `NessieCatalogTypeHandler`, `BigQueryMetastoreCatalogTypeHandler`**: All extract identifiers from already-loaded config maps without any I/O.
- **`OutputStatisticsOutputDatasetFacetBuilder`**: Reads three pre-computed `long` values from a `ConcurrentHashMap` populated incrementally during task completion callbacks. No accumulator iteration at build time.
- **`SparkJobDetailsFacetBuilder`, `SparkApplicationDetailsFacetBuilder`**: Read a small fixed set of named keys via direct `Properties.get()` / `conf.get(key, default)` calls. Trivially cheap.
- **`SnowflakeSaveIntoDataSourceCommandDatasetBuilder`**: Pure options-map extraction. No JDBC connections, no network calls, no reflection.
- **`InsertIntoHadoopFsRelationVisitor`**: Delegates identifier construction to `PathUtils.fromCatalogTable` or `PathUtils.fromPath`. No IO in the visitor itself.

---

## Methodology Notes

This audit covered 14 functional groups comprising approximately 60 source files from the OpenLineage Spark integration (`integration/spark/`). Source files examined include: the core visitor infrastructure (`QueryPlanVisitor`, `AbstractQueryPlanDatasetBuilder`, `OpenLineageRunEventBuilder`), all major data source handlers (LogicalRelation/V1, DataSourceV2, Iceberg, Delta, Databricks, Hive, JDBC, Kafka, Kinesis, MongoDB, Snowflake, BigQuery, GCP, Cosmos, Kusto), the complete column-level lineage engine (`ColumnLevelLineageBuilder`, `ExpressionTraverser`, all operator and expression visitors, merge-into handlers), and facet builders. For each group, Spark source files (`TreeNode.scala`, `QueryPlan.scala`, `SessionCatalog.scala`, `HiveExternalCatalog.scala`, `V2SessionCatalog.scala`, `InMemoryFileIndex.scala`, `FileScanRDD.scala`, `RDD.scala`, `SparkConf.scala`, `RuntimeConfig.scala`, `DataSourceRDD.scala`, `CacheManager.scala`) and Iceberg source files (`RESTSessionCatalog.java`, `CachingCatalog.java`, `SparkCatalog.java`, `SparkPartitioningAwareScan.java`, `TableMetadata.java`) were also examined to confirm behavior of methods called from OpenLineage. All severity assessments are based on call-frequency analysis (per-event vs. per-node vs. per-query) combined with measured or estimated per-call cost (network RTT, reflection overhead, object allocation). No runtime profiling data was collected; all findings are based on static analysis.
