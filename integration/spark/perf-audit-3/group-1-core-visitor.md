# Performance Audit - Group 1: Core Visitor Infrastructure

## Summary

The core visitor infrastructure performs O(N_plan_nodes × V_visitors) work on every Spark listener
event, using uncached reflection calls inside the inner loop. A single SQL execution triggers
`buildRun` 4–5 times (SQLExecutionStart, SQLExecutionEnd, each JobStart/JobEnd, each
StageCompleted), so the true cost multiplier is 4–5× that. Three compounding issues drive the
overhead: (1) `getGenericSuperclass()` + `getActualTypeArguments()` reflection is called on every
`isDefinedAt` check with no caching, (2) `searchDependencies=true` builders each independently
traverse the entire optimized plan via `collect()`, and (3) the `PlanUtils.safeIsInstanceOf` helper
calls `Class.forName()` on every invocation without caching and contains an inverted
`isAssignableFrom` logic that silently never matches the intended check.

## Classes Audited

- `QueryPlanVisitor`: Base partial function; provides default reflection-based `isDefinedAt` and `toString`
- `AbstractQueryPlanDatasetBuilder`: Bridge between AbstractQueryPlan builders and QueryPlanVisitor; drives `searchDependencies` tree traversal
- `AbstractQueryPlanInputDatasetBuilder`: Input-specific builder base; gates on `SparkListenerEvent` type
- `AbstractQueryPlanOutputDatasetBuilder`: Output-specific builder base; gates on `SparkListenerEvent` type
- `OpenLineageRunEventBuilder`: Central orchestrator; calls `buildInputDatasets` and `buildOutputDatasets` per Spark event
- `BaseVisitorFactory`: Registers 20+ concrete visitor instances per SQL execution context
- `InternalEventHandlerFactory`: Loads visitors via ServiceLoader (once at startup); registers them into context collections

## Performance Issues Found

---

### Uncached Reflection in `isDefinedAt` Hot Path - Severity: HIGH

**Class**: `QueryPlanVisitor`, `AbstractQueryPlanDatasetBuilder`, `AbstractPartial`
**Location**:
- `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/QueryPlanVisitor.java:97-107`
- `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractQueryPlanDatasetBuilder.java:151-160`
- `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractPartial.java:26-35`

**Description**: The default `isDefinedAt(LogicalPlan)` in `QueryPlanVisitor` and the
`isDefinedAtLogicalPlan(LogicalPlan)` in `AbstractQueryPlanDatasetBuilder` both call
`getClass().getGenericSuperclass()` and `((ParameterizedType) …).getActualTypeArguments()` on every
invocation, with no result caching. `isDefinedAt` is called for every plan node during plan
traversal, making this the hottest path in the entire visitor infrastructure. Twenty visitor
subclasses in `shared/` do not override `isDefinedAt(LogicalPlan)` or `isDefinedAtLogicalPlan()` and
all use this reflection path.

**Root Cause**: The type argument (e.g. `InsertIntoHadoopFsRelationCommand`) is fixed at compile
time and does not change between calls. The result of `getGenericSuperclass()` and
`getActualTypeArguments()[0]` for a given class is invariant across all invocations. The JVM does
not cache these reflection calls; each call allocates `Type` objects and performs class metadata
lookups.

**Code Evidence**:
```java
// QueryPlanVisitor.java:97 — called for EVERY plan node on EVERY event
@Override
public boolean isDefinedAt(LogicalPlan x) {
    Type genericSuperclass = getClass().getGenericSuperclass();   // reflection, uncached
    if (!(genericSuperclass instanceof ParameterizedType)) {
        return false;
    }
    Type[] typeArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments(); // reflection
    if (typeArgs != null && typeArgs.length > 0) {
        Type arg = typeArgs[0];
        boolean isAssignable = ((Class) arg).isAssignableFrom(x.getClass());
        ...
    }
    return false;
}
// Same pattern repeated verbatim in AbstractQueryPlanDatasetBuilder.isDefinedAtLogicalPlan():151
// and AbstractPartial.isDefinedAt():26
```

**Quantitative impact**: With ~20 non-overriding visitors and ~20 plan nodes per query, each
`buildInputDatasets` call incurs 400+ reflection-based `isDefinedAt` checks. At 4–5 events per SQL
execution, that is 1,600–2,000 reflection calls per SQL execution just for type resolution.

**Recommendation**: Cache the resolved `Class<?>` target at construction time in each visitor as a
`private final Class<?> targetPlanType` field, set in the constructor via a single
`getGenericSuperclass()` call. The `isDefinedAt` body then becomes a single
`targetPlanType.isInstance(x)` call. The same pattern applies to
`AbstractQueryPlanDatasetBuilder.isDefinedAtLogicalPlan`. The `toString()` method in
`QueryPlanVisitor` (line 128) independently duplicates this same reflection logic and should also
use the cached value.

---

### O(N_nodes x V_builders) Plan Traversal Per Event from `searchDependencies=true` Builders - Severity: HIGH

**Class**: `AbstractQueryPlanDatasetBuilder`, `OpenLineageRunEventBuilder`
**Location**:
- `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractQueryPlanDatasetBuilder.java:72-75`
- `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/OpenLineageRunEventBuilder.java:257-270`

**Description**: At least 8 builders have `searchDependencies=true`:
`DataSourceV2RelationInputOnEndDatasetBuilder`, `DataSourceV2RelationInputOnStartDatasetBuilder`,
`DataSourceV2ScanRelationOnEndInputDatasetBuilder`, `DataSourceV2ScanRelationOnStartInputDatasetBuilder`,
`InMemoryRelationInputDatasetBuilder`, `SubqueryAliasInputDatasetBuilder`, `ViewInputDatasetBuilder`,
`CommandPlanVisitor`. Each, when triggered, independently calls
`qe.optimizedPlan().collect(visitor)` — a full O(N) DFS via `TreeNode.foreach`. On top of this,
`buildInputDatasets` performs its own full traversal via `qe.optimizedPlan().map(inputVisitor)`.
For a single START event, multiple `searchDependencies=true` builders may all fire together.

**Root Cause**: Each builder independently traverses the entire plan. There is no shared traversal
or visitor multiplexing across builders. If 4 `searchDependencies=true` builders are active for a
given event type, the plan is traversed 5 times total (4 builders + base map), each visiting every
node with O(V) visitor checks per node.

**Code Evidence**:
```java
// AbstractQueryPlanDatasetBuilder.java:72
if (searchDependencies) {
    return ScalaConversionUtils.fromSeq(qe.optimizedPlan().collect(visitor)).stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
}

// OpenLineageRunEventBuilder.java:265
ScalaConversionUtils.fromSeq(qe.optimizedPlan().map(inputVisitor))
```

Spark's `TreeNode.collect()` (confirmed in Spark source):
```scala
// TreeNode.scala:305
def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))  // visits every node unconditionally
    ret.toSeq
}
```

**Recommendation**: Replace the multiple independent `collect()` traversals with a single unified
pass. Merge all active `searchDependencies=true` builder visitors into a single composite
`PartialFunction` and call `optimizedPlan().collect(mergedVisitor)` once. Demultiplex results to the
correct builder after the single traversal. This reduces 5 traversals to 1 for a typical Spark 3
input processing event.

---

### Double `isDefinedAt` Evaluation in `PlanUtils.merge().apply()` - Severity: MEDIUM

**Class**: `PlanUtils` (inner anonymous `OpenLineageAbstractPartialFunction`)
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/PlanUtils.java:66-101`

**Description**: In `PlanUtils.merge()`, the `apply(T x)` method creates a new stream pipeline
that calls `PlanUtils.safeIsDefinedAt(pfn, x)` for every visitor in `fns`. This is a second
independent scan calling `isDefinedAt` on every visitor, redundant with the preceding call to
`merge.isDefinedAt(x)`. Since `AbstractPartialFunction.applyOrElse` calls `isDefinedAt` before
`apply`, every node check triggers `safeIsDefinedAt` twice across the full visitor list.

**Root Cause**: The `apply()` method uses `.filter(pfn -> PlanUtils.safeIsDefinedAt(pfn, x))` as
a separate filter pass rather than reusing any result already computed by `isDefinedAt()`.

**Code Evidence**:
```java
// PlanUtils.java:66 — isDefinedAt scan (first scan per applyOrElse call):
public boolean isDefinedAt(T x) {
    return fns.stream()
        .filter(pfn -> PlanUtils.safeIsDefinedAt(pfn, x))
        .findFirst().isPresent();
}

// PlanUtils.java:76 — apply() scan (second scan per applyOrElse call):
public Collection<D> apply(T x) {
    return fns.stream()
        .filter(pfn -> PlanUtils.safeIsDefinedAt(pfn, x))  // calls isDefinedAt AGAIN on each visitor
        .map(pfn -> { ... pfn.apply(x) ... })
        ...
}
```

**Recommendation**: Override `applyOrElse` directly in the merged function to avoid the implicit
double `isDefinedAt` check. Alternatively, in `apply()` skip the `.filter()` guard and instead
wrap `pfn.apply(x)` in a try/catch — since `safeApply` already handles exceptions — collecting
non-empty results. This trades a safety filter for avoiding double evaluation.

---

### `PlanUtils.safeIsInstanceOf` Calls `Class.forName()` Without Caching Plus Logic Bug - Severity: MEDIUM

**Class**: `PlanUtils`, `CreateReplaceDatasetBuilder`
**Location**:
- `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/PlanUtils.java:326-332`
- `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/CreateReplaceDatasetBuilder.java:56`

**Description**: `PlanUtils.safeIsInstanceOf(Object instance, String classCanonicalName)` calls
`Class.forName(classCanonicalName)` on every invocation. `CreateReplaceDatasetBuilder` calls this
from `isDefinedAtLogicalPlan()`, which is invoked on every plan node during traversal.
`Class.forName()` involves classloader lookup and is not free; it returns the same `Class` object
every time for a fixed class name string.

Additionally, the implementation contains an inverted logic bug:

**Code Evidence**:
```java
// PlanUtils.java:328-329 — Class.forName called every time, and logic is BACKWARDS
Class c = Class.forName(classCanonicalName);
return instance.getClass().isAssignableFrom(c);
// isAssignableFrom(c) returns true if instance's class is a SUPERTYPE of c
// This is the opposite of the intended "is instance an instance of that class"
// Correct form: c.isInstance(instance)  or  c.isAssignableFrom(instance.getClass())
```

**Recommendation**: (1) Fix the inverted `isAssignableFrom` to `c.isInstance(instance)`. (2) In
`CreateReplaceDatasetBuilder`, resolve the `Class` once in a static initializer and store as
`private static final Class<?> CREATE_V2_TABLE_CLASS` with null fallback if absent; use
`CREATE_V2_TABLE_CLASS != null && CREATE_V2_TABLE_CLASS.isInstance(x)` in `isDefinedAtLogicalPlan`.
Remove `Class.forName()` from the traversal hot path entirely.

---

### `UnknownEntryFacetListener` Serializes Plan Nodes on Every Event by Default - Severity: MEDIUM

**Class**: `UnknownEntryFacetListener`, `LogicalPlanSerializer`
**Location**:
- `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/UnknownEntryFacetListener.java:76-96`
- `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/LogicalPlanSerializer.java`

**Description**: `UnknownEntryFacetListener.build(root)` is called in `buildRunFacets` on every
event where `spark_unknown` is enabled (the default). It calls `root.collectLeaves()` — a full
O(N) DFS — and for each unvisited leaf and the root calls `planSerializer.serialize(x)`. The
serializer drives Jackson serialization through a `Proxy` object and `MethodUtils.invokeMethod`
(Apache Commons reflection) on each call. This runs for both the START and COMPLETE events of the
same SQL execution, since `clear()` is called after `build()` and the listener state is reset.

**Root Cause**: The `spark_unknown` facet collection strategy serializes every unmatched leaf and
root node through a reflection-heavy Jackson pipeline on every `buildRunFacets` call, regardless of
whether the plan has already been serialized for a prior event in the same execution.

**Code Evidence**:
```java
// UnknownEntryFacetListener.java:77
ScalaConversionUtils.fromSeq(root.collectLeaves()).stream()  // full DFS O(N)
    .map(this::mapEntry)  // per-leaf: x.outputSet(), x.inputSet(), planSerializer.serialize(x)
    ...

// LogicalPlanSerializer.serialize():
// Uses MethodUtils.invokeMethod(objectMapper, "writeValueAsString", x)
// where objectMapper is the non-shaded Jackson instance
```

**Recommendation**: (1) Gate `build()` to run at most once per query execution by caching on
execution ID in `OpenLineageContext` and skipping on subsequent calls. (2) Consider changing the
default for `spark_unknown` to disabled; most production deployments gain little from it while
paying serialization cost on every event.

---

### Misplaced Micrometer Timer Wraps Function Object Creation, Not Plan Traversal - Severity: LOW

**Class**: `OpenLineageRunEventBuilder`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/OpenLineageRunEventBuilder.java:308-325`

**Description**: The `visitLogicalPlan()` method wraps its body in
`meterRegistry.timer("openlineage.spark.dataset.input.execution.time").record(...)`. The lambda
body creates and returns a `scala.Function1` object. The actual plan traversal
(`qe.optimizedPlan().map(inputVisitor)`) happens after `visitLogicalPlan()` returns at the call
site in `buildInputDatasets`, outside the timed region. The timer records near-zero time (object
allocation cost) and provides no useful observability signal.

**Code Evidence**:
```java
// OpenLineageRunEventBuilder.java:308 — timer wraps only the Function1 construction
private <D> Function1<LogicalPlan, Collection<D>> visitLogicalPlan(...) {
    return openLineageContext.getMeterRegistry()
        .timer("openlineage.spark.dataset.input.execution.time")
        .record(() ->
            ScalaConversionUtils.toScalaFn(...));  // only Function1 creation is timed

// The untimed actual work, at line 265:
ScalaConversionUtils.fromSeq(qe.optimizedPlan().map(inputVisitor))
```

**Recommendation**: Move the timer around the full `buildInputDatasets` body (or at minimum around
the `qe.optimizedPlan().map(inputVisitor)` line) to capture real traversal time.

---

### `catalogTableFor` Issues Synchronous Metastore Call on Each Matching Plan Node - Severity: MEDIUM

**Class**: `QueryPlanVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/QueryPlanVisitor.java:77-88`

**Description**: `catalogTableFor(TableIdentifier tableId)` calls
`session.sessionState().catalog().getTableMetadata(tableId)` synchronously from `apply()`.
In production environments with a Hive Metastore (HMS) this issues a blocking Thrift RPC.
Seven visitor classes call this unconditionally when the plan node type matches:
`AlterTableAddColumnsCommandVisitor`, `AlterTableRenameCommandVisitor`,
`AlterTableSetLocationCommandVisitor`, `AlterTableAddPartitionCommandVisitor`,
`DropTableCommandVisitor`, `TruncateTableCommandVisitor`, `LoadDataCommandVisitor`.

**Root Cause**: The `CatalogTable` metadata is fetched from HMS without any per-execution or
per-invocation caching. For DDL-heavy workloads or where `buildRun` is called multiple times per
execution for the same table, this results in repeated blocking Thrift RPCs on the OpenLineage
processing thread.

**Code Evidence**:
```java
// QueryPlanVisitor.java:83
return Optional.of(session.sessionState().catalog().getTableMetadata(tableId));
// In HiveSessionCatalog, getTableMetadata issues a Hive Thrift call if not in local cache

// DropTableCommandVisitor.java:39 — unconditional on match
Optional<CatalogTable> table = catalogTableFor(command.tableName());
```

**Recommendation**: Add a `Map<TableIdentifier, Optional<CatalogTable>>` cache within
`OpenLineageContext` that persists for the lifetime of a single SQL execution context. Before
calling `getTableMetadata`, check the cache. This eliminates duplicate Thrift calls across multiple
events (START, COMPLETE, job events) for the same table within one execution.

## Clean Classes

- **`AbstractQueryPlanInputDatasetBuilder`**: Clean wrapper; `isDefinedAt(SparkListenerEvent)` returns `true` directly without reflection. The `delegate()` method is a straightforward delegation pattern with no hidden traversals.

- **`AbstractQueryPlanOutputDatasetBuilder`**: Clean; `isDefinedAt(SparkListenerEvent)` is direct. The `jobNameSuffixFromLogicalPlan()` operates on the optimized plan root only (no tree traversal). The `identToSuffix()` utility is pure string manipulation.

- **`InternalEventHandlerFactory`**: Clean at runtime. `ServiceLoader.load()` and visitor construction happen once at `ContextFactory` initialization, not per event. The `generate()` helper is a clean flatMap over factory list. No repeated I/O on the hot path.

- **`BaseVisitorFactory`**: Clean. `hasKafkaClasses()`, `hasHiveClasses()` etc. are class-availability checks evaluated at visitor list construction time (once per SQL execution context creation), not on the per-event hot path.

- **`OpenLineageRunEventTimeoutExecutor`**: Functionally correct with no hidden blocking issues beyond the intended timeout mechanism. The `ExecutorService` is shared via `getOrCreateExecutor()`, avoiding repeated thread pool creation. `TimeoutExecutor.run()` uses `Future.get(timeout, MILLISECONDS)` correctly.

## Spark/Iceberg Internals Investigated

- **`spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala`**:
  `collect()`, `map()`, `foreach()`, and `flatMap()` all perform a full DFS traversal visiting every
  node. There is no short-circuit mechanism. `collect()` uses `pf.lift` which calls `isDefinedAt`
  then `apply` per node. `map()` calls `foreach` and applies the function to every node
  unconditionally, including interior nodes where no visitor will match.

- **`spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/QueryPlan.scala`**:
  `optimizedPlan` is wrapped in `LazyTry` — repeated calls are cheap after first computation.
  `semanticHash()` delegates to `canonicalized.hashCode()` where `canonicalized` is a
  `TransientBestEffortLazyVal`: lock-free, cached after first computation in live Spark operation
  (cache is `transient` so cleared across Java serialization, but irrelevant for in-process use).
  `doCanonicalize()` involves full expression normalization (expensive first call, cached
  thereafter). `outputSet()` and `inputSet()` are also `TransientBestEffortLazyVal` cached — cheap
  after first access.

- **`spark/core/src/main/java/org/apache/spark/util/TransientBestEffortLazyVal.java`**:
  Lock-free implementation using `VarHandle.compareAndSet`. The compute function may run multiple
  times under contention (best-effort, not exactly-once). In live Spark operation the cached value
  persists for the lifetime of the plan node object. No locking overhead on the read path.

- **`spark/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala`**:
  `optimizedPlan`, `analyzed`, and `executedPlan` are all lazily evaluated via `LazyTry`.
  `qe.executedPlan()` — called in the `isLogPlanAsJson` debug path — materializes the full physical
  planning pipeline on first access. In production, Spark will have already triggered this as part
  of query execution, making the call cheap. However the subsequent `.toJSON()` call on the physical
  plan is a full recursive plan serialization that is not lazy, adding observable latency even when
  Spark has pre-computed the plan.
