# OpenLineage Spark Integration — Validated Performance Audit Report

This report contains only findings validated through source code inspection and realistic cost
estimation. Marginal findings (sub-millisecond aggregate impact on realistic workloads) are
excluded. Where the original audit was factually wrong, corrections are noted explicitly.

Scale assumptions used for impact estimates:
- Plan size: 50–200 nodes
- Schema width: 100–500 columns
- Active datasets per query: 20–50 tables
- Events per SQL execution: 5–15 (SQL start/end + job start/end + stage events)
- Execution frequency: up to 1,000 SQL executions/hour (streaming / high-frequency ETL)
- Expression depth: 15–50 levels for complex analytical queries

---

## Critical Issues (Must Fix)

---

### 1. GCP Metadata Service — Multiple Uncached HTTP Calls Per Event

**Component**: `GCPUtils`, `GcpJobFacetBuilder`, `GcpRunFacetBuilder`
**Scope**: All Dataproc BATCH users

`GCPUtils.fetchGCPMetadata()` makes a live HTTP GET to `http://metadata.google.internal/computeMetadata/v1`
on every invocation. There is no caching at any level — no static field, no volatile flag, no
memoization. Both `GcpJobFacetBuilder` and `GcpRunFacetBuilder` fire on every Spark listener event
and each delegates to `GCPUtils`, making 3–4 HTTP calls per builder per event.

Additionally, `identifyResource()` fetches the batch/session ID endpoint purely to determine
resource type via `.isPresent()`, then discards the result. The caller (`getDataprocRunFacetMap`,
`createDataprocOriginMap`) immediately calls the same getter again — doubling the HTTP call count
for that endpoint.

**Impact**: 6–8 blocking HTTP calls per Spark event. Each call to the GCP metadata service from a
Dataproc VM takes 2–10ms. Total per event: **12–80ms of added driver latency**. A job with 15 events
per SQL execution adds **180ms–1.2 seconds per SQL execution** purely from metadata fetches.
This blocks the Spark event bus thread.

**Fix**: Cache all metadata values in `static volatile` fields populated on first invocation.
GCP instance metadata (project ID, region, batch ID, batch UUID) does not change during a job's
lifetime. Also refactor `identifyResource()` to return the fetched value alongside the resource
type, eliminating the duplicate fetch in each call path.

---

### 2. Databricks DBFS Mount Point Enumeration Per Job Start

**Component**: `DatabricksEnvironmentFacetBuilder`
**Scope**: All Databricks users

`getDatabricksMountpoints()` reflectively calls `DbUtils.mounts()` on every `SparkListenerJobStart`
with no caching between calls. This API enumerates all DBFS mount points via a synchronous call to
the Databricks DBFS service, blocking the Spark event bus thread. On workspaces with dozens to
hundreds of mount points, the duration is non-deterministic.

**Impact**: Synchronous DBFS API call on every Spark job start. No caching. Duration proportional
to mount point count and DBFS service latency. Directly adds to driver latency per job.

**Fix**: Cache the result after the first call. Mount points are stable during a running job.
Also consider making mount-point collection opt-in via a configuration flag.

---

### 3. Delta `loadTable()` Called Twice Per Event Per Delta Table

**Component**: `DeltaHandler`
**Scope**: All Delta Lake users

`DeltaHandler.getDatasetIdentifier()` (line 68) and `DeltaHandler.getDatasetVersion()` (line 132)
each call `deltaCatalog.loadTable(identifier)` independently. The `DataSourceV2Relation` already
carries the fully resolved `DeltaTableV2` object in its `table` field — Spark loaded it during
query analysis. OpenLineage strips this resolved object before reaching the handler and then
re-fetches it twice.

Unlike Iceberg, Delta has no `CachingCatalog` equivalent. Each call to `DeltaCatalog.loadTable()`
for cloud-backed Delta tables (S3, GCS, ADLS) may read the `_delta_log/` directory from object
storage to determine the current snapshot.

**Impact**: 2 `loadTable` calls per Delta table per event. At 15 events per SQL execution and
20 Delta tables: **600 `loadTable` invocations per SQL execution**. For cloud-backed Delta logs:
5–50ms per call depending on object storage latency → up to **30 seconds of added latency per
SQL execution** in the worst case.

**Note on Iceberg**: The original audit flagged Iceberg's `loadTable()` as equally problematic.
This is incorrect for standard deployments. Iceberg's `SparkCatalog` wraps the underlying catalog
with a `CachingCatalog` (Caffeine-backed, enabled by default, 30-second sliding TTL). OpenLineage
calls `loadTable()` on the same `SparkCatalog` singleton that Spark's analyzer used — the result
is a Caffeine in-memory cache hit (~200ns). Iceberg `loadTable()` is only a real network call when:
(a) the cache TTL has expired (queries running >30 seconds between planning and lineage emission),
(b) the table was replaced via `REPLACE TABLE AS SELECT` (which explicitly invalidates the cache),
or (c) the cache is disabled via `cache-enabled=false`.

**Fix for Delta**: Pass the already-resolved `DeltaTableV2` from `relation.table()` through the
call chain to `DeltaHandler`. Extract the snapshot from the `DeltaLog` already loaded in that
object. This eliminates both `loadTable` calls per event.

---

### 4. `CatalogUtils3.getHandlers()` Rebuilt on Every Call

**Component**: `CatalogUtils3`
**Scope**: All users of DataSourceV2 (Iceberg, Delta, Databricks, JDBC)

`CatalogUtils3.getHandlers(context)` is a private static method that is never cached. Every
invocation instantiates 6 `CatalogHandler` objects (including `new IcebergHandler(context)` which
itself allocates a 6-element `catalogTypeHandlers` list in its constructor), then calls
`hasClasses()` on each handler. Each `hasClasses()` call invokes `ClassLoader.loadClass()` for the
handler's target class.

This method is called from `getDatasetIdentifier()`, `getStorageDatasetFacet()`,
`getCatalogDatasetFacet()`, and `getDatasetVersion()` — a minimum of 4 times per dataset per event.

**Impact at scale**: 50 tables × 4 calls/table × 15 events = **3,000 `getHandlers()` invocations
per SQL execution**. Each invocation allocates ~12 short-lived objects and makes 4 classloader
calls. Total: **36,000 object allocations and 12,000 classloader calls per SQL execution**.
At 1,000 executions/hour: 36 million object allocations per hour from this one call site alone,
driving continuous Eden GC pressure and measurable minor GC frequency.

**Fix**: Cache the filtered handler list as a `static volatile List<CatalogHandler>`.
Since classpath availability cannot change at runtime, the list is stable for the JVM lifetime.
Initialize lazily with double-checked locking on first use. Separately, cache the `hasClasses()`
boolean result for each handler as a `static final boolean` — each probe needs to run exactly once
at class initialization.

---

### 5. HMS Thrift RPC for Managed Hive Table Default Location

**Component**: `PathUtils`, `InsertIntoHiveTableVisitor`, `HiveTableRelationVisitor`,
`CreateHiveTableAsSelectCommandVisitor`, `OptimizedCreateHiveTableAsSelectCommandVisitor`
**Scope**: Hive Metastore users with managed tables

When a managed Hive table has no explicit `storage.locationUri`, `PathUtils.getDefaultLocationUri()`
calls `sparkSession.sessionState().catalog().defaultTablePath(identifier)`. Inside Spark's
`SessionCatalog.defaultTablePath()`, this chains to `getDatabaseMetadata(db)` →
`HiveExternalCatalog.getDatabase(db)` → two synchronized HMS Thrift RPCs: `requireDbExists`
(which calls `databaseExists`) followed by `getDatabase`. Neither result is cached between calls.

**Scope clarification vs. original audit**: This RPC only fires when `storage.locationUri` is
absent from the `CatalogTable`. For existing tables loaded through normal HMS resolution, Spark
populates `locationUri` at load time via `HiveClientImpl`, so the RPC does not fire for standard
SELECT or INSERT into existing tables. The RPC fires in DDL-heavy paths (CREATE TABLE AS SELECT,
certain V2 catalog operations) where location isn't yet resolved.

**Impact**: 2 HMS Thrift RPCs per invocation where the RPC fires. Each HMS RPC: 5–50ms on a
loaded metastore. `V2SessionCatalogHandler.loadNamespaceMetadata()` also fires an equivalent
`getDatabase` RPC unconditionally on every V2 catalog dataset resolution, even when the table
location is already available from relation properties.

**Fix**: Replace `defaultTablePath()` with pure string arithmetic:
`<warehouse>/<database>.db/<table>` — the same formula Hive uses and the same result Spark would
return. The warehouse path is already available via `PathUtils.getWarehouseLocation()`. This
eliminates the HMS round-trips for all DDL paths. For `V2SessionCatalogHandler`, make
`loadNamespaceMetadata()` conditional on `PROP_LOCATION` being absent from relation properties.

---

### 6. O(D²) BFS in `ColumnLevelLineageBuilder.findDependentInputs`

**Component**: `ColumnLevelLineageBuilder`
**Scope**: All users with column-level lineage enabled

`findDependentInputs()` implements BFS over the expression-dependency graph using a
`LinkedList<Dependency>` as the visited set. The deduplication filter on every BFS iteration is:

```java
.filter(dependency -> !dependentInputs.contains(dependency))
```

`LinkedList.contains()` is O(D) — a full sequential scan of the list. As the BFS frontier grows,
each new candidate check scans the entire accumulated visited set. Total cost for one output
column's BFS: O(D²) where D is the total dependency chain depth. `Dependency` already implements
correct `equals()`/`hashCode()` via `Objects.hash(exprId, transformationInfo)`.

`findDependentInputs()` is called once per output column from `buildFields()`, and additionally
once per join/filter/sort/aggregate node from `datasetDependencyInputs()`, with results not
memoized between calls.

**Impact at scale**: W=500 output columns, D=200 dependency depth:
- Per-column BFS: D×(D+1)/2 = 20,100 comparisons
- Across 500 columns: **10,050,000 `LinkedList` element comparisons per `buildFields()` call**
- Each comparison invokes `Dependency.equals()` (~20ns): **~200ms per `buildFields()` call**
- At 5 CLL-relevant events per SQL execution: **~1 second added per SQL execution** from BFS alone

**Fix**: Replace `new LinkedList<>()` with `new HashSet<>()`. This is a one-line change. Reduces
BFS cost from O(D²) to O(D) per output column. Since `Dependency.equals()`/`hashCode()` are
already implemented correctly, this is a safe, drop-in replacement.

---

### 7. `ExpressionTraverser.copyFor()` Allocates New `VisitorFactory` and 18 Visitor Objects Per Recursive Step

**Component**: `ExpressionTraverser`, `VisitorFactory`
**Scope**: All users with column-level lineage enabled on queries with complex expressions

Every call to `copyFor()` — the primary recursion mechanism in expression traversal — calls
`ExpressionTraverser.of()`, which hardcodes `new VisitorFactory()`. `VisitorFactory.expressionVisitors()`
then constructs `Arrays.asList(new AliasVisitor(), new CaseWhenVisitor(), ...)` — 6 new objects.
`operatorVisitors()` constructs 12 more. All 18 visitor classes are stateless.

The static `VisitorFactory` singleton in `ExpressionDependencyCollector` (line 19) correctly avoids
this, but `ExpressionTraverser` itself does not reuse it.

**Impact at scale**: Complex expression trees with depth 15–20 and branching factor 2–3 (nested
COALESCE, CASE WHEN with multiple branches, window functions inside aggregates) at 500 output
columns:

- Per expression tree at depth 15, branching 2: up to 2^10 interior nodes × 7 objects = ~7,000
  object allocations per column expression
- Across 500 output columns: **~3.5 million short-lived object allocations per CLL event**
- Eden allocation: ~175MB per SQL execution from this pattern alone
- At 1,000 executions/hour with a 256MB Eden space: **~700 minor GC pauses per hour**, each
  adding 20–50ms stop-the-world latency → **14–35 seconds of GC pause per hour** from this
  single pattern

**Fix**: Pass `this.visitorFactory` through `copyFor()` instead of calling `ExpressionTraverser.of()`.
Since all visitor instances are stateless, a single shared `VisitorFactory` per traversal root is
correct. Alternatively, make `VisitorFactory.expressionVisitors()` and `operatorVisitors()` return
`static final` immutable lists — then `new VisitorFactory()` is cheap and the returned lists are
never re-allocated.

---

### 8. `InputFieldsCollector` and `OutputFieldsCollector` Have O(N×S) Cost at Wide Schemas

**Component**: `InputFieldsCollector`, `OutputFieldsCollector`, `ColumnLevelLineageBuilder`
**Scope**: Column-level lineage on wide schemas (100+ columns)

Two distinct O(S) patterns compound during every CLL traversal:

**`InputFieldsCollector.extractInternalInputs()`**: For each qualifying leaf node (data source),
calls `ScalaConversionUtils.fromSeq(node.output())` to copy the full output attribute list from
Scala to Java, then iterates all S attributes calling `builder.addInput()` S times. This makes
each qualifying leaf node visit O(S), not O(1). For a plan with L leaf nodes and S columns:
one `InputFieldsCollector` pass is O(L × S).

**`ColumnLevelLineageBuilder.addOutput()`** (called from `OutputFieldsCollector`): Each call does:
```java
schema.getFields().stream()
    .filter(field -> field.getName().equals(attributeName))
    .findAny()
```
This is an O(S) linear scan through schema fields to find a field by name. It is called once per
output attribute registered. With S output attributes registered: O(S²) total per CLL event.

Additionally there is a case-sensitivity inconsistency: `addOutput` uses `.equals()` while
`getInputsUsedFor()` uses `.equalsIgnoreCase()` — a latent correctness bug.

**Impact at scale**: S=500 columns, plan with 20 leaf nodes, 4 CLL traversals per event:
- `InputFieldsCollector`: 20 leaves × 500 attrs × 4 traversals = 40,000 `addInput()` calls per
  event, each with a Scala-to-Java copy of 500 elements first
- `addOutput()`: 500 × 500 = 250,000 string comparisons per event from the linear schema scan
- 4 independent traversals multiply all costs by 4
- At 500-column schemas, the per-node cost is ~2µs at leaf nodes (not 50ns as typically assumed
  for pure dispatch), making the total CLL traversal cost **~700µs per event**
- At 15 events/execution: **~10ms per SQL execution** from traversal overhead alone

**Fix**: Build a `Map<String, SchemaDatasetFacetFields>` index at `ColumnLevelLineageBuilder`
construction time. Replace all O(S) `schema.getFields()` scans with O(1) map lookups. Fix the
case-sensitivity inconsistency. For `InputFieldsCollector`, consider whether registering all S
attributes is always necessary or if a lazy approach is possible.

---

### 9. Four Independent Full Plan Traversals in CLL Engine

**Component**: `ColumnLevelLineageUtils` (spark3), `ExpressionDependencyCollector`,
`InputFieldsCollector`, `OutputFieldsCollector`
**Scope**: All users with column-level lineage enabled

`buildColumnLineageDatasetFacet` triggers four separate full DFS traversals over the logical plan:
1. `OutputFieldsCollector.collect` — recursive DFS
2. `ExpressionDependencyCollector.collect` — `plan.foreach`
3. `InputFieldsCollector.collect` — recursive DFS
4. An additional `plan.foreach` scanning for `InMemoryRelation` nodes

For cached datasets, passes 2 and 3 recurse into each cached plan's original tree, potentially
re-running on plans that have already been visited.

This compounds with Issues 7 and 8: each additional traversal multiplies all per-node schema-width
costs.

**Impact**: At 200 nodes, 500-column schema, each traversal carrying O(S) per-leaf work: reducing
from 4 traversals to 1 eliminates 3× the traversal cost and 3× the GC pressure from Finding 7.

**Fix**: Unify into a single `plan.foreach` dispatch that routes each node simultaneously to all
three collectors. The primary ordering constraint (outputs must be collected before dependencies
can be resolved) can be addressed by a two-pass approach (one pass for output collection, one
unified pass for dependency and input collection), reducing traversals from 4 to 2.

---

### 10. `MergeIntoDeltaColumnLineageVisitor` Double- and Triple-Traverses Source/Target Subtrees

**Component**: `MergeIntoDeltaColumnLineageVisitor`
**Scope**: Delta MERGE INTO operations with CLL enabled

`collectInputs()` explicitly calls `InputFieldsCollector.collect(context, target)` and
`InputFieldsCollector.collect(context, source)` when it encounters a `MergeIntoCommand` node.
However, `InputFieldsCollector.collect` is itself a recursive traversal, and the outer
`ExpressionDependencyCollector.collect(plan)` driving the entire CLL analysis uses `plan.foreach()`
which already visits all children of `MergeIntoCommand` including `target` and `source`.

Additionally, `collectExpressionDependencies()` calls
`ColumnLevelLineageUtils.collectInputsAndExpressionDependencies(context, source)` at line 72,
which runs `ExpressionDependencyCollector.collect(source)` and `InputFieldsCollector.collect(source)`
again — a third full pass over the source subtree.

Beyond the performance impact, this is a correctness-adjacent bug: if `addInput()` is not
fully idempotent for all code paths, the duplicate traversals can produce duplicate lineage entries.

**Impact**: Source subtree traversed ≥3×, target subtree ≥2×. For MERGE operations against large
tables with complex source queries (the common streaming Delta MERGE pattern), this multiplies
all CLL traversal costs for the source plan by 3×.

**Fix**: Remove the explicit `InputFieldsCollector.collect(target)`, `collect(source)`, and
`collectInputsAndExpressionDependencies(source)` calls from the merge visitor. Allow the outer
`plan.foreach` to handle child traversal naturally. Only the merge-action-to-output mapping
logic (the `addDependency` calls from `getMergeActions`) should remain in the visitor.

---

### 11. `LogicalRelationDatasetBuilder` — Unconditional Filesystem `isFile()` Call and Double `inputFiles()`

**Component**: `LogicalRelationDatasetBuilder` (shared module)
**Scope**: Users reading large partitioned tables via LogicalRelation

**`isFile()` call**: `isSingleFileRelation()` calls
`path.getFileSystem(hadoopConfig).getFileStatus(path).isFile()` — a blocking remote metadata call
against HDFS, S3, GCS, or ADLS — whenever a `HadoopFsRelation` has exactly one root path.
The `FileIndex` embedded in the relation already knows this information.

**Double `inputFiles()` call**: Lines 201 and 210 call `relation.inputFiles()` twice — once for
the null check and once to access `.length`. For relations backed by `CatalogFileIndex`, each call
triggers a full recursive filesystem listing (`listLeafFiles`). For large partitioned tables with
100,000+ files, each `inputFiles()` call also allocates a `String[100000]` (~6MB of file URI
strings).

**Impact at scale**: At 100,000 files per relation:
- Two `inputFiles()` calls = two full filesystem listings + **12MB of transient String array
  allocation** per `apply()` invocation
- For S3 (1,000 files per ListObjectsV2 page): 100 HTTP requests × 20ms = 2 seconds per
  `inputFiles()` call → **4 seconds of S3 listing latency** per invocation from the double call
- The `isFile()` call adds a synchronous HDFS/S3 `getFileStatus` round-trip (5–50ms) for any
  single-file relation

**Fix**: (1) Call `relation.inputFiles()` exactly once, store in a local variable, use it for both
the null check and `.length`. (2) Replace `isSingleFileRelation()`'s `getFileStatus().isFile()` call
with inspection of the `FileIndex.leafFiles` cache already held by the relation — no I/O required.

---

### 12. Uncached `isDefinedAt` Reflection in Visitor Hot Path

**Component**: `QueryPlanVisitor`, `AbstractQueryPlanDatasetBuilder`, `AbstractPartial`
**Scope**: All users; fires on every plan node for every visitor for every event

`QueryPlanVisitor.isDefinedAt(LogicalPlan x)` (lines 97–110) and
`AbstractQueryPlanDatasetBuilder.isDefinedAtLogicalPlan()` both call
`getClass().getGenericSuperclass()` and `((ParameterizedType) ...).getActualTypeArguments()` on
every invocation with no caching. The generic type argument is fixed at compile time — it never
changes between calls for a given visitor subclass — but the JVM returns a newly constructed
`ParameterizedType` wrapper on each `getGenericSuperclass()` call.

Approximately 23 concrete subclasses use the base reflection path (11 of 34 `QueryPlanVisitor`
subclasses already override `isDefinedAt` with direct `instanceof` checks and are unaffected).

**Impact at scale**: 23 visitors × 200 plan nodes × 15 events = **69,000 reflection calls per
SQL execution**. At ~100ns per call: **~7ms per SQL execution**. At 1,000 executions/hour:
**~7 CPU-seconds per hour** on the driver, devoted entirely to re-resolving type arguments
that never change. Additionally, `QueryPlanVisitor.toString()` (line 128) performs the same
reflection independently.

Note: this finding was downgraded to marginal in an earlier analysis that assumed only 4 events
per execution and 20 nodes. At 15 events and 200 nodes, the call count is ~37× higher.

**Fix**: Cache the resolved `Class<?>` in each visitor as `private final Class<?> targetPlanType`,
set once in the constructor via a single `getGenericSuperclass()` call. The `isDefinedAt` body
becomes `targetPlanType.isInstance(x)` — zero reflection, zero allocation. Apply the same pattern
to `AbstractQueryPlanDatasetBuilder.isDefinedAtLogicalPlan()`.

---

## Significant Issues (Should Fix)

---

### 13. Uncached Class-Existence Probes at 10+ Sites

**Component**: `DatasetVersionDatasetFacetUtils`, `DeltaHandler`, `IcebergHandler`,
`AbstractDatabricksHandler`, `TableContentChangeDatasetBuilder`, `SqlDWDatabricksVisitor`,
`SnowflakeRelationVisitor`, `SnowflakeColumnLineageVisitor`, `KustoRelationVisitor`

Every `hasClasses()` / `isXyzClass()` / `hasSqlDWDatabricksClasses()` guard method performs a
live `ClassLoader.loadClass()` or `Class.forName()` probe on every invocation. The classpath
cannot change at runtime; the answer is fixed at JVM startup.

`TableContentChangeDatasetBuilder.isDefinedAtLogicalPlan()` is the most egregious instance:
it allocates `new IcebergHandler(context)` twice per call (each constructor allocates a
6-element handler list), calling `hasClasses()` on each. Since `isDefinedAtLogicalPlan()` is
called for every node during plan traversal, this fires hundreds of times per event.

**Impact**: Contributes to the `CatalogUtils3.getHandlers()` object allocation storm (Finding 4).
Individually each probe is ~200ns, but collectively across 10+ sites called per-node or
per-dataset, they add up. At scale, the aggregate classloader contention is measurable.

**Fix**: Replace every `ClassLoader.loadClass(name)` probe in a guard method with a
`static final boolean HAS_XYZ_CLASSES` field initialized once in a static block:
```java
private static final boolean HAS_DELTA_CLASSES;
static {
    boolean b = false;
    try { Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog"); b = true; }
    catch (Exception ignored) {}
    HAS_DELTA_CLASSES = b;
}
```
Apply this pattern to all 10+ sites. This is the most widespread single fixable pattern in
the codebase.

---

### 14. `SparkPropertyFacetBuilder` Copies Entire Spark Config on Every Event

**Component**: `SparkPropertyFacetBuilder`

`buildFacet()` calls `conf.getAll()` which clones the entire `ConcurrentHashMap` of Spark
configuration entries into a new array on every invocation. On large clusters (Databricks,
EMR, Dataproc), Spark configuration routinely contains 500–2,000 entries including YARN, HDFS,
S3, and cluster-specific settings. Only 2 entries are needed by default (`spark.master`,
`spark.app.name`).

Additionally, `session.conf().get(item)` throws `NoSuchElementException` for missing keys,
caught silently — paying full stack trace construction cost per absent property per event.
With customized `capturedProperties` containing keys that don't always exist, this fires
repeatedly.

**Impact**: At 1,000 config entries × 15 events/execution × 1,000 executions/hr = 15,000
`conf.getAll()` calls/hr. Each call: ~100µs (array allocation + iteration over 1,000 entries)
= **1.5 CPU-seconds/hr** from config copying alone, plus exception overhead if properties
are absent.

**Fix**: Replace `conf.getAll()` with direct per-key lookups using `conf.getOption(key)` for
only the keys in `allowedProperties`. Eliminate the exception-as-control-flow pattern.

---

### 15. `IcebergHandler` Calls `session.conf().getAll()` Twice Per Dataset Per Event

**Component**: `IcebergHandler`

`getDatasetIdentifier()` (line 130) and `getCatalogDatasetFacet()` (line 83) each call
`session.conf().getAll()` independently, materializing the full Spark runtime configuration as
a Java `Map` twice per Iceberg dataset per event.

**Impact**: Two full config-map copies per Iceberg table per event. Compounds with
`CatalogUtils3.getHandlers()` rebuild (Finding 4) — every `getHandlers()` call instantiates a
new `IcebergHandler`, making the two `conf.getAll()` calls part of the per-handler-rebuild
overhead.

**Fix**: Cache the extracted catalog-specific property subset per catalog name. Since Spark
configuration is effectively immutable after session start, any caching of the extracted result
is safe. Store in `OpenLineageContext` or in the `IcebergHandler` instance (if instance
lifetime is extended beyond per-event).

---

### 16. `PlanUtils.safeIsInstanceOf` Calls `Class.forName()` With Inverted Logic Bug

**Component**: `PlanUtils`, `CreateReplaceDatasetBuilder`

`PlanUtils.safeIsInstanceOf(Object instance, String classCanonicalName)` contains an inverted
`isAssignableFrom` check:
```java
instance.getClass().isAssignableFrom(c)
```
This asks "is the instance's concrete class a supertype of c?" — the opposite of the intended
check. The result is that this guard **silently never matches** its intended condition.
`CreateReplaceDatasetBuilder` calls this from `isDefinedAtLogicalPlan()` on every plan node,
with `Class.forName()` called on each invocation with no caching.

**Impact**: Two bugs in one: (1) a performance issue (uncached `Class.forName()` per node),
and (2) a correctness bug (the guard never triggers). Any behavior that depends on this check
passing is broken.

**Fix**: Correct the logic to `c.isInstance(instance)`. Cache the `Class<?>` reference as a
`static final` field in `CreateReplaceDatasetBuilder`, resolved once at class initialization.

---

## Findings Downgraded or Refuted

The following findings from the original audit were found to be incorrect or marginal after
source code verification:

**Iceberg `loadTable()` as a network call (original Finding 3, Iceberg path)**: Incorrect for
default configurations. Iceberg's `SparkCatalog` wraps underlying catalogs with `CachingCatalog`
(Caffeine, enabled by default, 30s sliding TTL). OpenLineage uses the same `SparkCatalog`
singleton as Spark's planner. Post-planning `loadTable()` calls are Caffeine cache hits (~200ns).
Network calls only occur on TTL expiration (>30s idle), `REPLACE TABLE AS SELECT` (which
explicitly invalidates the cache), or when `cache-enabled=false`.

**HMS Thrift RPC for all Hive table reads (original Finding 6)**: Overstated. The RPC only fires
when `CatalogTable.storage.locationUri` is absent. For existing tables loaded through normal HMS
resolution, Spark populates `locationUri` at load time. The RPC fires in DDL-heavy paths (CREATE
TABLE AS SELECT), not for standard SELECT or INSERT into existing tables.

**`InsertIntoHiveTableVisitor` makes 5 `PathUtils.fromCatalogTable` calls**: Incorrect. The code
uses exclusive `if/else` branches — only one branch executes per `apply()` invocation.

**`spark_unknown` facet serialization**: `"spark_unknown"` is in `SparkOpenLineageConfig.DISABLED_BY_DEFAULT`.
Not relevant for default deployments. The `UnknownEntryFacetListener` also uses
`LogicalPlanSerializer` which has a 50,000-character truncation guard (unlike
`LogicalPlanRunFacetBuilder` which bypasses it).

**`debug` facet `scanLogicalPlan()` O(N²)**: `"debug"` is also in `DISABLED_BY_DEFAULT`. If
enabled, the O(N²) string generation from `node.toString()` (which calls `treeString()`,
rendering the entire subtree for each node) generates megabytes of intermediate strings. But
it is disabled by default and the fix (replace `node.toString()` with `node.simpleString()`)
is straightforward.

**`LogicalPlanRunFacetBuilder` full plan serialization**: `"spark.logicalPlan"` is in
`DISABLED_BY_DEFAULT`. However, there is a latent correctness gap: `LogicalPlanFacet.getPlan()`
calls `plan.toJSON()` at Jackson serialization time (not at `build()` time), which completely
bypasses the 50,000-character truncation guard in `LogicalPlanSerializer`. Any user who enables
this facet on a production cluster with large plans (20–50MB serialized) will experience
seconds of serialization overhead per event and hundreds of MB of transport per SQL execution,
with no safety net.

**Multiple plan traversals overhead (original Finding 2, pure dispatch cost)**: When evaluated
as pure 50ns-per-node dispatch, this is marginal. It becomes real only when per-node work is
O(S) due to schema-width effects (see Finding 8/9 above). The traversal count itself is not
the bottleneck — the work done inside each traversal step at wide schemas is.

---

## Priority Order

| Priority | Finding | Expected Impact |
|---|---|---|
| 1 | GCP Metadata HTTP calls (Finding 1) | 180ms–1.2s per SQL execution on Dataproc |
| 2 | Databricks DBFS mount enumeration (Finding 2) | Blocking driver latency per job start |
| 3 | Delta `loadTable()` double-call (Finding 3) | Up to 30s added latency/execution for cloud Delta |
| 4 | O(D²) BFS `LinkedList` → `HashSet` (Finding 6) | ~200ms per CLL build at W=500, D=200; one-line fix |
| 5 | `CatalogUtils3.getHandlers()` caching (Finding 4) | 36K objects/exec, GC pressure; one-line fix |
| 6 | `ExpressionTraverser` static VisitorFactory (Finding 7) | ~700 GC pauses/hr at production scale |
| 7 | `InputFieldsCollector` O(N×S) schema scan (Finding 8) | ~10ms per exec at 500-col schemas |
| 8 | Unify 4 CLL traversals (Finding 9) | 4× multiplier on all CLL schema-width costs |
| 9 | `isDefinedAt` reflection caching (Finding 12) | ~7ms per exec at 200 nodes, 15 events |
| 10 | `LogicalRelationDatasetBuilder` double `inputFiles()` (Finding 11) | 12MB alloc + 4s S3 latency at 100K files |
| 11 | Merge visitor duplicate traversals (Finding 10) | 3× source subtree traversal for MERGE + CLL |
| 12 | `static final` class-existence probes (Finding 13) | Reduces `CatalogUtils3` rebuild cost; systemic hygiene |
| 13 | `SparkPropertyFacetBuilder` conf.getAll() (Finding 14) | 1.5 CPU-sec/hr at large clusters |
| 14 | `IcebergHandler` double `conf.getAll()` (Finding 15) | Compounds with Finding 4 |
| 15 | `safeIsInstanceOf` correctness bug (Finding 16) | Correctness fix; blocks a broken code path |

---

## Addendum: Re-estimation at Extreme Plan Scale

The estimates above used 50–200 plan nodes and 100–500 columns. The following analysis applies the
same findings to a real observed production plan with these dimensions:

```
Nodes total:          6,778      (≈34× baseline)
Nodes leaf (Relation):  310      (≈6× baseline)
Max depth:              134
Output columns total: 29,850
Unique output col IDs:  790
Unique input col refs:  315

Node type breakdown:
  Project               1,258
  SerializeFromObject     981
  MapElements             913
  DeserializeToObject     913
  Filter                  858
  Relation                310
  AppendColumns           282
  Union                   259
  Join                    224
  Aggregate               205
  Repartition             175
  SubqueryAlias           174
  ResolvedHint             87
  MapGroups                72
  Deduplicate              60
  TypedFilter               5
  InsertIntoHadoopFsRelationCommand  1
  Generate                  1
```

### Findings that escalate to catastrophic at this scale

**Finding 12 — isDefinedAt uncached reflection**

At 200 nodes: 23 visitors × 200 × 15 events = 69,000 calls → ~7ms/exec (Significant)

At 6,778 nodes: 23 × 6,778 × 15 = **2,338,830 calls → ~234ms/exec** (Critical)

The call count scales linearly with node count. At 1,000 executions/hr this is ~234 CPU-seconds/hr
on the driver devoted entirely to re-resolving generic type arguments that never change.
Priority moves from #9 to top-5.

**Finding 7 — ExpressionTraverser allocation storm**

At 200 nodes / 500-col schemas: ~3.5M objects/exec → ~700 minor GC pauses/hr

This plan has 1,258 Project nodes averaging ~24 output expressions each (29,850 total / 1,258),
plus 858 Filter predicates, 205 Aggregates with AggregateExpression children, and 2,807 Dataset
API encoder nodes (SerializeFromObject/MapElements/DeserializeToObject) each containing
ObjectType-based encoder trees. All recurse through `copyFor()` which allocates a new
`VisitorFactory` + 18 visitor objects at every recursive step.

Conservative estimate at expression depth 5:
- Projects: 1,258 × 24 expressions × 5 depth × 6 objects = 909,600 objects per CLL event
- Filters: 858 × 10 × 5 × 6 = 257,400
- Aggregates: 205 × 15 × 5 × 6 = 92,250
- Dataset encoder nodes (2,807): 2,807 × 10 × 3 × 6 = 505,260

**Total: ~1.76M visitor objects per CLL event × 15 events = ~26M objects per SQL execution**

At 32 bytes each: ~830MB of garbage per SQL execution. At 1,000 exec/hr with a 256MB Eden space:
**~3,250 minor GC pauses/hr**, each 20–50ms stop-the-world = **65–163 seconds of GC pause per
hour** from this single pattern. The driver is spending more time collecting garbage than doing
work.

Additionally, the 134 max plan depth combined with recursive expression traversal creates
composite call stacks that could approach JVM stack depth limits for deeply-nested plans.

**Finding 4 — CatalogUtils3.getHandlers() rebuilds**

At 50 relations: 50 × 4 × 15 = 3,000 rebuilds → 12,000 ClassLoader.loadClass() calls

At 310 relations: 310 × 4 × 15 = **18,600 rebuilds → 111,600 ClassLoader.loadClass() calls**

At 10µs per loadClass: **~1.1s/exec** from classloader probes alone. At 50µs (contended class
loading on a large Spark classpath): **~5.6s/exec**. This may be the dominant non-network
overhead at this plan size.

**Finding 9 — Four independent CLL traversals compounded by O(S) per-leaf work**

At 200 nodes / 20 leaves / 500 cols: ~10ms/exec pure traversal overhead

At 6,778 nodes / 310 leaves / 96 cols per Relation (29,850 / 310):
- InputFieldsCollector: 310 × 96 × 4 traversals × 15 events = 1,786,800 `addInput()` calls/exec,
  each preceded by a Scala→Java sequence materialization of ~96 elements
- addOutput() O(S²) scan: 790 output cols × 790 schema fields = 624,100 string comparisons/event
  × 15 events = 9.36M comparisons/exec → ~47ms/exec from schema scans alone
- Total CLL traversal overhead: **several seconds per SQL execution**

### Node type-specific concerns

**259 Union nodes**: CLL maps each output column through all Union branches. Deeply nested Unions
(this plan likely has multi-level Union stacks given 259 instances at depth 134) multiply the
number of dependency edges in the BFS graph. The effective D in the O(D²) BFS may exceed the 315
unique input col refs bound if intermediate expression nodes are counted as distinct Dependency
objects. The O(D²) estimate of ~200ms could be conservative.

**2,807 Dataset API nodes (SerializeFromObject / MapElements / DeserializeToObject)**: Each
contains ObjectType encoder expressions — one encoder tree per field in the serialized class. At
96 output columns per Relation, each SerializeFromObject likely encodes ~96 fields, generating a
deep encoder expression tree (~3 nodes per field × 96 = 288 expression nodes per encoder node).
The ExpressionTraverser visits all of these. This is the dominant driver of the allocation storm
above and was not present in the 200-node baseline (which assumed standard SQL plans without
Dataset[T] operations).

**224 Join nodes**: JoinVisitor checks each child independently. With 224 joins, CLL registers
join conditions as dependencies for every output column that flows through a join. This compounds
the BFS work in `findDependentInputs`.

### Revised priority table for extreme-scale plans

| Finding | Estimate at 200-node baseline | Estimate at 6,778-node plan |
|---|---|---|
| ExpressionTraverser allocation (F7) | ~700 GC pauses/hr | **~3,250 GC pauses/hr; 65–163s GC/hr — GC crisis** |
| CatalogUtils3 classloader (F4) | ~12K calls, GC pressure | **111K calls; ~1.1–5.6s/exec** |
| isDefinedAt reflection (F12) | ~7ms/exec | **~234ms/exec** |
| 4 CLL traversals + O(S) (F8+F9) | ~10ms/exec | **Several seconds/exec** |
| O(D²) BFS (F6) | ~200ms/exec | ~200ms–1s/exec (Union fan-out may increase) |
| GCP HTTP calls (F1) | 180ms–1.2s/exec | Same (per-event, not per-node) |
| Delta loadTable() (F3) | Up to 30s/exec | Same (per-table, not per-node) |

At this plan scale, the combination of Findings 4 + 7 + 9 + 12 produces **multiple seconds of
pure CPU overhead per SQL execution** before any network calls, plus a GC crisis that degrades
all other JVM work on the driver. Fixing these four findings — all of which are simple mechanical
changes (HashSet swap, static field caching, singleton VisitorFactory, one traversal instead of
four) — would likely reduce OpenLineage overhead by 80–90% for plans of this complexity.

---

## Methodology

All findings in this report were validated by:
1. Reading the cited source files directly from the cloned repository at `~/perf-audit/OpenLineage`
2. Tracing call chains into Spark source (`~/perf-audit/spark`) and Iceberg source
   (`~/perf-audit/iceberg`) to verify actual behavior
3. Estimating costs using realistic production assumptions (wide schemas, many tables, high
   event frequency) rather than small-job defaults
4. Cross-checking "disabled by default" claims against `SparkOpenLineageConfig.DISABLED_BY_DEFAULT`

Findings excluded from this report: those with sub-millisecond aggregate impact at production
scale, those where the original audit was factually wrong about the code path, and those guarded
by `DISABLED_BY_DEFAULT` (unless there is a correctness issue, as with `LogicalPlanRunFacetBuilder`'s
bypassed size guard).
