# Performance Audit - Group 12: Facet Builders

## Summary

The most critical performance issues are concentrated in `LogicalPlanRunFacetBuilder` and `DebugRunFacetBuilderDelegate`. `LogicalPlanRunFacetBuilder` calls `plan.toJSON()` on the full optimized plan tree, which recursively serializes every node with all constructor parameters including schema data, producing potentially massive JSON output. `DebugRunFacetBuilderDelegate.scanLogicalPlan()` compounds this by calling `node.toString()` (which equals `treeString()`, a full recursive tree walk) for every individual node while itself walking the entire tree, creating O(n²) string generation. Both facets are disabled by default, but enabling either under a complex query is very expensive. The remaining builders (`SparkPropertyFacetBuilder`, `DatabricksEnvironmentFacetBuilder`) have moderate concerns; the others are clean.

## Classes Audited

- `LogicalPlanRunFacetBuilder`: Builds `spark.logicalPlan` run facet from `QueryExecution.optimizedPlan()`, disabled by default
- `DebugRunFacetBuilder`: Builds `debug` run facet; gates on facet-disable check, disabled by default; fires for every Spark event if enabled
- `DebugRunFacetBuilderDelegate`: Does all heavy lifting for debug facet: walks logical plan, scans classpath, collects metrics, reads memory stats
- `OutputStatisticsOutputDatasetFacetBuilder`: Reads pre-aggregated job write stats from `JobMetricsHolder`
- `SparkPropertyFacetBuilder`: Emits selected Spark config properties; fires on every `SparkListenerEvent`
- `SparkJobDetailsFacetBuilder`: Extracts job ID and three properties from `SparkListenerJobStart.properties()`
- `SparkApplicationDetailsFacetBuilder`: Reads a handful of fixed Spark config keys from `SparkContext`
- `SparkProcessingEngineRunFacetBuilder`: Emits Spark version string, delegates to `SparkProcessingEngineRunFacetBuilderDelegate`
- `DatabricksEnvironmentFacetBuilder`: Collects Databricks-specific env vars, job properties, and DBFS mount points via reflection

---

## Performance Issues Found

### Full Plan Serialization via `plan.toJSON()` - Severity: HIGH

**Class**: `LogicalPlanRunFacetBuilder` / `LogicalPlanFacet`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/LogicalPlanFacet.java:28`
**Description**: `LogicalPlanFacet.getPlan()` calls `plan.toJSON()` on the full optimized logical plan. This serializes the entire plan tree to compact JSON at Jackson serialization time (every time the event is emitted).
**Root Cause**: `TreeNode.toJSON()` in Spark (`TreeNode.scala:1135`) calls `jsonValue`, which recursively visits every node in the plan tree via `collectJsonValue`. For each node, `jsonFields` iterates all constructor parameters (via `productIterator`) serializing schema data, expression IDs, table metadata, and nested `TreeNode` children. For complex queries with many joins, subqueries, or wide schemas this produces megabytes of JSON. The method is called at Jackson serialization time, not at `build()` time, so there is no opportunity to short-circuit early.
**Code Evidence**:
```java
// LogicalPlanFacet.java:26-29
@JsonRawValue
public String getPlan() {
    return plan.toJSON();  // O(plan_size) recursive serialization of entire tree
}
```
```scala
// Spark TreeNode.scala:1139-1152
private def jsonValue: JValue = {
  val jsonValues = scala.collection.mutable.ArrayBuffer.empty[JValue]
  def collectJsonValue(tn: BaseType): Unit = {
    val jsonFields = ("class" -> ...) :: ("num-children" -> ...) :: tn.jsonFields
    jsonValues += JObject(jsonFields)
    tn.children.foreach(collectJsonValue)  // full recursive walk of all children
  }
  collectJsonValue(this)
  jsonValues
}
```
**Recommendation**: This facet is already disabled by default (`DISABLED_BY_DEFAULT` in `SparkOpenLineageConfig`). Reinforce documentation warning about payload size. If users must enable it, add a node-count or byte-size limit before serialization (similar to `DebugRunFacetSerializer`'s `payloadSizeLimitInKilobytes` guard), or capture only the top-N nodes of the plan tree. Alternatively, serialize at `build()` time rather than deferred to Jackson, so size checks can happen before the event is emitted.

---

### O(n²) Plan Walking in `scanLogicalPlan` via `node.toString()` - Severity: HIGH

**Class**: `DebugRunFacetBuilderDelegate`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/builder/DebugRunFacetBuilderDelegate.java:176`
**Description**: `scanLogicalPlan()` recursively walks every node in the logical plan tree and stores `node.toString()` as the `desc` field for each `LogicalPlanNode`. `LogicalPlan.toString()` delegates to `treeString()` in Spark, which itself recursively walks the entire subtree rooted at that node. For a plan with N nodes, this produces N full subtree text serializations, resulting in O(n²) total string work.
**Root Cause**: `TreeNode.toString()` returns `treeString()` (verbose mode), which calls `generateTreeString` recursively on all children and `innerChildren`. When called from `scanLogicalPlan` on node k, it generates a string representation for the entire subtree below node k. Since `scanLogicalPlan` calls this for every node during its own recursive walk, the total string work is the sum of all subtree sizes — O(n²) for a linear chain, and O(n²) on average for any branching tree.
**Code Evidence**:
```java
// DebugRunFacetBuilderDelegate.java:173-188
private List<LogicalPlanNode> scanLogicalPlan(LogicalPlan node) {
    List<LogicalPlanNode> result = new ArrayList<>();
    LogicalPlanNodeBuilder builder =
        LogicalPlanNode.builder().id(nodeId(node)).desc(node.toString()); // treeString() = full subtree walk
    // ...
    ScalaConversionUtils.fromSeq(node.children()).stream()
        .forEach(child -> result.addAll(scanLogicalPlan(child)));  // recursive walk of all children
    return result;
}
```
```scala
// Spark TreeNode.scala:961
override def toString: String = treeString  // generates full indented subtree string
```
**Recommendation**: Replace `node.toString()` with `node.simpleString(SQLConf.get.maxToStringFields)` or just `node.nodeName()`, which generates only a single-line description of the node without recursing into children. The `LogicalPlanNode.desc` field is intended to identify the node, not reproduce the full subtree. This reduces the complexity from O(n²) to O(n).

---

### Double Serialization in `DebugRunFacetSerializer` - Severity: MEDIUM

**Class**: `DebugRunFacetSerializer`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/serializer/DebugRunFacetSerializer.java:41-46`
**Description**: The serializer always fully serializes the debug facet once with `OpenLineageClientUtils.toJson()` to measure its byte size, and then—if within limits—serializes it again via `jsonGenerator`. Every enabled debug facet event therefore incurs two full serializations, doubling the CPU and memory allocation cost.
**Root Cause**: The size check requires a complete serialization to a `String` before deciding whether to emit. There is no streaming size estimator or single-pass path.
**Code Evidence**:
```java
// DebugRunFacetSerializer.java:41-46
int payloadSize =
    OpenLineageClientUtils.toJson(new DebugRunFacetWithStandardSerializer(facet))  // FIRST full serialization to String
            .getBytes(StandardCharsets.UTF_8)
            .length / 1024;
// ... if within limit, proceeds to write via jsonGenerator  // SECOND full serialization
```
**Recommendation**: Use a `CountingOutputStream` wrapped around a null sink, driving Jackson's `JsonGenerator` directly, to measure the serialized byte count in a single pass without allocating the full string. Alternatively, serialize to a `byte[]` once and write that directly to `jsonGenerator` via `writeRawValue()`, so the payload is only produced once regardless of outcome.

---

### `SparkPropertyFacetBuilder` Iterates All Spark Config Entries on Every Event - Severity: MEDIUM

**Class**: `SparkPropertyFacetBuilder`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/builder/SparkPropertyFacetBuilder.java:63-75`
**Description**: `buildFacet()` calls `conf.getAll()`, which returns the entire Spark configuration as a `(String, String)[]` array, then streams through it. A production Spark cluster routinely has hundreds of config entries. This builder has no `isDefinedAt` override, so by default (via `AbstractPartial`) it fires for all `SparkListenerEvent` instances. Additionally, it calls `SparkSession.active()` and `session.conf().get(item)` per allowed property, where `getConfString` throws a `NoSuchElementException`-derived exception for missing keys that is silently caught, incurring exception-creation overhead on every event for every absent property.
**Root Cause**: `SparkConf.getAll()` (`SparkConf.scala:499`) copies all settings from an internal `ConcurrentHashMap` to a new array on each invocation. Even with only 2 default allowed properties, the full scan is O(all_config_entries). The subsequent `session.conf().get(item)` call (`RuntimeConfig.scala:53` → `SQLConf.getConfString:8261`) throws when the key is absent, meaning exception construction runs per-property per-event.
**Code Evidence**:
```java
// SparkPropertyFacetBuilder.java:63-65
Arrays.stream(conf.getAll())  // allocates array of ALL config entries (hundreds in production)
    .filter(t -> allowedProperties.contains(t._1))
    .forEach(t -> m.putIfAbsent(t._1, t._2));
// ...
allowedProperties.forEach(item -> m.putIfAbsent(item, session.conf().get(item)));
// session.conf().get() throws for missing keys — caught at line 76 silently
```
**Recommendation**: Replace `conf.getAll()` with direct per-key lookups: `allowedProperties.forEach(key -> conf.getOption(key).foreach(v -> m.put(key, v)))`. This is O(allowed_properties) instead of O(all_config_entries). For `session.conf()`, use `session.conf().getOption(item)` (returning `Option[String]`) to avoid exception-as-control-flow.

---

### `DatabricksEnvironmentFacetBuilder` Enumerates All DBFS Mount Points Synchronously - Severity: MEDIUM

**Class**: `DatabricksEnvironmentFacetBuilder`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/builder/DatabricksEnvironmentFacetBuilder.java:116-155`
**Description**: `getDatabricksMountpoints()` reflectively instantiates `DbfsUtilsImpl` and calls `mounts()` on it. The Databricks `DbUtils.mounts()` API enumerates all DBFS mount points, which internally involves a call to the Databricks DBFS service. This happens synchronously on the Spark event bus thread during `SparkListenerJobStart` processing.
**Root Cause**: `DbUtils.mounts()` performs I/O to enumerate storage mount configurations. Running this inside a Spark listener callback blocks the event bus thread for the duration of the call. The latency is non-deterministic (depends on DBFS service load), and on workspaces with many mounts (dozens to hundreds) the result is a large list to serialize. No caching is applied; the call repeats on every job start.
**Code Evidence**:
```java
// DatabricksEnvironmentFacetBuilder.java:144-148
List<Object> mountsList =
    ScalaConversionUtils.fromSeq(
        (Seq<Object>) ReflectionUtils.tryExecuteMethod(dbfsUtils, "mounts").get()); // potential RPC to DBFS service
for (Object mount : mountsList) { /* iterate all mounts */ }
```
**Recommendation**: Cache the mount point list after the first call (mount points change rarely during a running job). Add a configurable timeout. Consider making mount-point collection opt-in via a config flag (e.g., `spark.openlineage.databricks.collectMountPoints=false`).

---

### `DebugRunFacetBuilder` Fires on Every Event When Enabled - Severity: LOW

**Class**: `DebugRunFacetBuilder`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/builder/DebugRunFacetBuilder.java:30-32`
**Description**: `isDefinedAt()` returns `true` for every object (no type filtering) when the debug facet is enabled. The full `DebugRunFacetBuilderDelegate.buildFacet()` (including logical plan walk, classpath scan, metrics collection, and memory stats) runs for every single `SparkListenerEvent`, not just job-end events.
**Root Cause**: The type parameter for `DebugRunFacetBuilder` is `Object` with no event type restriction, and `isDefinedAt` only checks the facet-disable flag.
**Code Evidence**:
```java
// DebugRunFacetBuilder.java:30-32
@Override
public boolean isDefinedAt(Object x) {
    return !FacetUtils.isFacetDisabled(openLineageContext, "debug");  // true for ALL events if enabled
}
```
**Recommendation**: Restrict `isDefinedAt` to `SparkListenerJobEnd` or `SparkListenerSQLExecutionEnd` events. The debug facet is disabled by default, so this is a lower-priority hardening, but it prevents accidental severe overhead if a user enables it.

---

## Clean Classes

**`OutputStatisticsOutputDatasetFacetBuilder`**: Clean. Fires only for `SparkListenerJobEnd` and `SparkListenerSQLExecutionEnd`. All write metrics are pre-aggregated incrementally by `JobMetricsHolder.addMetrics()` during `onTaskEnd` callbacks (summing `TaskMetrics` scalars), so `build()` simply reads three pre-computed `long` values from a `ConcurrentHashMap` lookup. No accumulator iteration occurs in the builder itself.

**`SparkJobDetailsFacetBuilder`**: Clean. Fires only for `SparkListenerJobStart`. Reads exactly three `String` values from `job.properties()` (a `java.util.Properties`) using named key lookups. Trivially cheap.

**`SparkApplicationDetailsFacetBuilder`**: Clean. Guards against irrelevant events with an early return (only builds for `ApplicationStart`, `JobStart`, `SQLExecutionStart`). Reads a small fixed set of named Spark config keys via direct `conf.get(key, default)` calls — no full config scan.

**`SparkProcessingEngineRunFacetBuilder`**: Clean. Fires on all `SparkListenerEvent`s but only reads a version string cached in the delegate at construction time. The entire `buildFacet()` call is O(1) with no I/O or collection iteration.

---

## Spark/Iceberg Internals Investigated

- **`~/perf-audit/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala`**:
  - `toJSON()` (line 1135): calls `compact(render(jsonValue))`, recursively visiting every tree node and serializing all constructor parameters via `productIterator`. Includes schema data, `ExprId`s, `CatalogTable` metadata, and nested `TreeNode` children. For plans with wide schemas or many nodes this produces large output.
  - `toString()` (line 961): delegates to `treeString()`, which calls `generateTreeString` recursively on all children and inner children (subqueries, CTEs). Produces a full indented text representation of the entire subtree rooted at the node — O(subtree_size) per call.
  - `generateTreeString()` (line 1047): walks both `children` and `innerChildren`, so the effective tree size can be larger than just the direct children chain.

- **`~/perf-audit/spark/core/src/main/scala/org/apache/spark/SparkConf.scala`**:
  - `getAll()` (line 499): copies all settings from the internal `ConcurrentHashMap` into a new `Array[(String, String)]` on each invocation. On a typical cluster this can be hundreds of entries.

- **`~/perf-audit/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala`**:
  - `getConfString(key)` (line 8261): throws `QueryExecutionErrors.sqlConfigNotFoundError` when the key is absent in both the settings map and registered config entries. Confirmed that `SparkPropertyFacetBuilder` will hit this exception for every missing property key on every event.

- **`~/perf-audit/spark/sql/core/src/main/scala/org/apache/spark/sql/classic/RuntimeConfig.scala`**:
  - `get(key)` (line 53): delegates directly to `SQLConf.getConfString(key)`, confirming exception-as-control-flow for missing keys when `SparkPropertyFacetBuilder` calls `session.conf().get(item)`.
