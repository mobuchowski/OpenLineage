# OpenLineage Spark Performance Audit — Large Plan Calibration Report

## Production Plan Statistics

| Metric | Value |
|---|---|
| Nodes total | 6,778 |
| Leaf (Relation) nodes | 310 |
| Non-leaf nodes | 6,468 |
| Max depth | 134 |
| Output columns total | 29,850 |
| Unique output ExprIds | 790 |
| Unique input column refs | 315 |
| Spark listener events per SQL exec (estimated) | 15 |

### Node type breakdown

| Node type | Count |
|---|---|
| Project | 1,258 |
| SerializeFromObject | 981 |
| MapElements | 913 |
| DeserializeToObject | 913 |
| Filter | 858 |
| Relation (leaf) | 310 |
| AppendColumns | 282 |
| Union | 259 |
| Join | 224 |
| Aggregate | 205 |
| Repartition | 175 |
| SubqueryAlias | 174 |
| ResolvedHint | 87 |
| MapGroups | 72 |
| Deduplicate | 60 |
| TypedFilter | 5 |
| InsertIntoHadoopFsRelationCommand | 1 |
| Generate | 1 |

This is a heavy Dataset[T] pipeline. The 981 + 913 + 913 + 282 + 72 = **3,161 Dataset-API nodes** (SerializeFromObject, DeserializeToObject, MapElements, AppendColumns, MapGroups) account for 47% of non-leaf nodes. These are opaque object-transformation nodes and require special attention for how OpenLineage handles them.

---

## Baseline Assumptions Used in Sub-Audits (Pre-Calibration)

Sub-audit groups assumed approximately 200 plan nodes per query. The production plan has 6,778 nodes — **34× larger**. Costs that scale linearly with plan size are therefore 34× higher than sub-audit estimates. This report recalibrates each finding to the production plan.

---

## Key Pre-Investigation Finding: CLL Schema Size Limit Does Not Protect This Plan

`ColumnLevelLineageUtils.isSchemaExceedsLimit` checks `schemaFacet.getFields().size() > schemaSizeLimit` (default 1,000). The `schemaFacet` reflects only the **final output schema** of the root node (likely 96 or so fields for the target table), not the 29,850 intermediate column references. With 310 Relations each having ~96 fields, the root schema is well under 1,000. **CLL will run on every SQL execution event** for this plan.

---

## Part 1: Plan-Size-Scaling Findings (O(N) with N = 6,778 nodes)

---

### F-1: Uncached Reflection in `isDefinedAt` Hot Path

**Source**: Group 1 — Severity originally: HIGH

**Original finding**: `QueryPlanVisitor.isDefinedAt(LogicalPlan)` calls `getClass().getGenericSuperclass()` and `getActualTypeArguments()` on every plan node visit, with no caching. Approximately 20 non-overriding visitors use this path.

**Verification of visitor count**: Inspection of `BaseVisitorFactory.getOutputVisitors()` and `Spark3VisitorFactoryImpl.getOutputVisitors()` shows approximately 20 output visitors registered per event. Of the 70 non-test files extending visitor base classes, 44 override `isDefinedAt` while 26 do not and fall through to the reflective default. In a typical Spark3 execution, approximately 23 active visitors use the reflective path (input + output combined, including Spark3-specific builders).

**Scaled cost arithmetic**:

- Reflective-path visitors: ~23
- Nodes per traversal: 6,778
- Full traversals per event (base `optimizedPlan().map()` call): 1
- searchDependencies builders performing an independent `collect()`: `LogicalRelationDatasetBuilder` (searchDependencies=true) + `InMemoryRelationInputDatasetBuilder` + `SubqueryAliasInputDatasetBuilder` = ~3 additional traversals
- Total traversal passes per event: ~4 (confirmed in Group 1 and `Spark3DatasetBuilderFactory`)
- Events per SQL execution: 15

Reflection calls per execution:
```
23 visitors × 6,778 nodes × 4 traversals × 15 events = 9,356,280 reflection calls/exec
```

Each `getGenericSuperclass()` + `getActualTypeArguments()` pair allocates `Type[]` objects and performs class metadata lookups. At 50–100 ns each (JVM warm, no JIT elimination due to virtual dispatch):
```
9,356,280 × 75 ns = ~702 ms/exec
```

Group 1 estimated 1,600–2,000 reflection calls at baseline. The production plan produces roughly **4,600× more** reflection calls than the sub-audit's qualitative estimate, though the sub-audit's "quantitative impact" section understated the issue even at baseline.

**Rating at production scale**: CRITICAL

**Note**: The double `isDefinedAt` evaluation from `PlanUtils.merge()` (Group 1, medium finding) doubles this cost further to approximately **1.4 seconds/exec** just for `isDefinedAt` reflection.

---

### F-2: Four Independent Full Plan Traversals Per CLL Invocation

**Source**: Group 6 — Severity originally: HIGH

**Original finding**: `ColumnLevelLineageUtils.buildColumnLineageDatasetFacet` triggers four full O(N) traversals:
1. `OutputFieldsCollector.collect` (manual recursive DFS)
2. `ExpressionDependencyCollector.collect` (`plan.foreach`)
3. `InputFieldsCollector.collect` (manual recursive DFS)
4. `plan.foreach` for InMemoryRelation scan

**Source verification**: Confirmed in `ColumnLevelLineageUtils.java:61-62` — `OutputFieldsCollector.collect` followed by `collectInputsAndExpressionDependencies` which internally runs `ExpressionDependencyCollector.collect` + `InputFieldsCollector.collect` + a fourth `plan.foreach` for InMemoryRelation. CLL runs once per `buildOutputDatasets` event, not per the 15 events, but the plan has 310 Relations and the pipeline may trigger CLL on each qualifying end event (SQLExecutionEnd and JobEnd = roughly 2 times/exec).

**Scaled cost arithmetic**:

Each traversal visits every node. At each non-leaf node, the operator-visitor list (12 visitors) is checked:

```
4 traversals × 6,778 nodes × 15 events (note: CLL fires ~2 times, not 15)
= 4 × 6,778 × 2 = 54,224 node visits for CLL traversals/exec
```

Wait — CLL fires on `buildColumnLineageDatasetFacet` which is called from `buildOutputDatasets`, which fires per qualifying event. Examining the flow: `OpenLineageRunEventBuilder.buildOutputDatasets` is called for `SQLExecutionEnd`, `JobEnd`, and similar events. Approximately 4 qualifying end events per SQL execution. CLL runs 4 times:

```
4 traversals × 6,778 nodes × 4 CLL invocations = 108,448 node visits for CLL/exec
```

At each visit, `ExpressionDependencyCollector.collectFromOperator` checks 12 operator visitors via `isDefinedAt`. For Dataset-API nodes (SerializeFromObject, DeserializeToObject, MapElements, MapGroups, AppendColumns), none of the 12 operator visitors match — they will all return false from their `isDefinedAt`, which is `instanceof Union`, `instanceof Project`, etc. These Dataset-API nodes consume 12 × 3,161 = **37,932 wasted `isDefinedAt` checks per traversal pass** of the full plan.

**`addOutput` and `addInput` call volume** (the O(S) schema scan per call):

For the 4 CLL invocations, OutputFieldsCollector calls `addOutput` for each node's output attributes. The 6,778 nodes have a combined total of 29,850 output column references. With a schema of 790 unique fields (the output ExprId count), each `addOutput` call scans up to 790 entries:

```
29,850 addOutput() calls × 790 schema entries per scan (worst case) = 23,581,500 comparisons per CLL invocation
× 4 invocations = ~94 million comparisons/exec just for addOutput schema scans
```

This is almost certainly the dominant CLL cost at this plan scale. The sub-audit estimated O(N × W × S) but did not compute numbers for a 6,778-node plan.

**Rating at production scale**: CRITICAL

---

### F-3: `ExpressionTraverser` Visitor Allocation Storm

**Source**: Group 6 and 7 — Severity originally: MEDIUM/HIGH

**Original finding**: Every `copyFor()` call in `ExpressionTraverser` creates a new `VisitorFactory` + 18 visitor objects. This fires once per expression node per CLL traversal.

**Dataset encoder expression trees**: Inspecting Spark source at `SerializerBuildHelper.scala:407-420`, a `ProductEncoder` (case class) generates per field:
- An `Invoke` expression (to call the getter)
- A recursive `createSerializer` call (typically `StaticInvoke`, `If/IsNull`, etc.)
- For a nullable field: an `If(IsNull(...), null, nonNullOutput)` wrapper

For a typical case class with `F` fields, the serializer expression tree has approximately `3F + 2` nodes (one `Invoke` + one serializer expression + one `If` wrapper per field, plus a `CreateNamedStruct` root). For F = 96 fields, that is roughly **290 expression nodes per SerializeFromObject serializer tree** and similarly for DeserializeToObject.

Crucially: the `ExpressionDependencyCollector.collectFromOperator` calls `operatorVisitors().stream().filter(v -> v.isDefinedAt(operator))` for every plan node. For a `SerializeFromObject` node, the `ProjectVisitor` would check `operator instanceof Project` → false, `FilterVisitor` checks `instanceof Filter` → false, etc. None match. **No ExpressionTraverser is invoked for SerializeFromObject/MapElements/DeserializeToObject nodes** because no operator visitor handles them.

However: `InputFieldsCollector.discoverInputsFromNode` also processes each node. For a `SerializeFromObject`, it falls through all the `instanceof` checks (not a DataSourceV2Relation, not a HiveTableRelation, not a LogicalRelation, not a LeafNode) and returns `Collections.emptyList()`. This is correct behavior — no ExpressionTraverser is invoked.

**Conclusion**: The ExpressionTraverser allocation storm does NOT apply to the 3,161 Dataset-API nodes because no operator visitor fires on them. The allocation storm only affects nodes that match an operator visitor. With 1,258 Project + 858 Filter + 205 Aggregate + 224 Join + 259 Union = **2,804 nodes that DO trigger visitors**, and each triggering expression traversal on their expression lists.

For a typical Project with 96 output expressions, ProjectVisitor calls `ExpressionTraverser.of(expr, ...)` for each expression, which creates 1 `VisitorFactory` + 6 expression visitor objects per expression node traversal. With an average expression depth of ~3 and branch factor 2, each projection column traversal may visit ~7 expression nodes, each calling `copyFor()` which creates another 18 visitor objects.

Rough estimate per CLL invocation:
```
1,258 Project nodes × 96 avg expressions × 7 expr nodes per expression × 18 visitor objects = 15,226,416 visitor object allocations
```
This is significant GC pressure per CLL invocation. With 4 invocations/exec: **~61M visitor object allocations/exec**.

**Rating at production scale**: CRITICAL (GC pressure, not computation time — but GC pauses during event handling block listener callbacks)

---

### F-4: `addOutput` O(S) Schema Scan (Expanded from F-2)

**Source**: Group 6 and 7 — Severity originally: MEDIUM

This finding was already integrated into F-2 above due to its dominance at production scale. To isolate it:

`ColumnLevelLineageBuilder.addOutput(ExprId, String)` performs a linear scan over all schema fields to find a match by name. This is called once per output attribute per plan node.

**Scaled arithmetic**:
```
29,850 total output column references × O(790) schema scan per call
= ~23.6M string comparisons per CLL invocation
× 4 invocations/exec = ~94M string comparisons/exec
```

**Rating at production scale**: CRITICAL — must be fixed with a `Map<String, SchemaDatasetFacetFields>` index.

---

### F-5: BFS Visited-Set O(D²) in `findDependentInputs`

**Source**: Group 6, 7, 8 — Severity originally: HIGH

**Original finding**: `ColumnLevelLineageBuilder.findDependentInputs` uses `LinkedList.contains` (O(D)) as its visited-set guard, producing O(D²) total work.

**Production scale reasoning**: The dependency graph has 790 unique output ExprIds. For each of the final output columns (at the root), the BFS traverses the expression-dependency chain from root output ExprId back to leaf input ExprIds. The 259 Union nodes are significant here: each Union node maps each child column's ExprId to the first child's ExprId (via UnionVisitor). With 259 Unions in a binary chain and each Union exposing K columns, the dependency chain length for a single output column can be up to 259 hops deep through the Union chain alone. D (dependencies per output column) can easily reach 300–500.

```
D ≈ 400 (estimate for a deeply unioned pipeline)
W = 790 output columns
O(D²) per findDependentInputs call × W calls = 790 × 400² = 126,400,000 operations per buildFields()
× 4 invocations/exec = ~505M operations/exec
```

This is likely the single largest algorithmic bottleneck in CLL at this plan scale. Switching to `HashSet<Dependency>` makes this O(D) per call:
```
After fix: 790 × 400 = 316,000 operations per buildFields() — a 400× improvement
```

**Rating at production scale**: CRITICAL

---

### F-6: `datasetDependencyInputs` Repeated BFS for Dataset-Level Dependencies

**Source**: Group 6 — Severity originally: MEDIUM

`datasetDependencyInputs()` runs `findDependentInputs` for each entry in `datasetDependencies`. Every `Filter` (858), `Sort` (0 in this plan, but the 205 Aggregates create entries), and `Join` (224) node registers one synthetic ExprId in `datasetDependencies`.

Approximate count of `datasetDependencies` entries: 858 (Filter) + 205 (Aggregate) + 224 (Join) = **1,287 entries**.

Without the `HashSet` fix (F-5):
```
1,287 BFS calls × O(D²) each = 1,287 × 400² = ~206M operations just for dataset dependencies
```

With `HashSet` fix:
```
1,287 × 400 = ~515,000 operations — acceptable
```

**Rating at production scale**: CRITICAL (before F-5 fix), Significant (after F-5 fix, but still worth addressing duplicate invocation from `buildFields` and `buildDatasetDependencies`).

---

### F-7: `UnionVisitor` LinkedList Indexed Access with 259 Unions

**Source**: Group 8 — Severity originally: LOW

`UnionVisitor.apply()` stores child attributes in a `LinkedList<ArrayList<Attribute>>` then accesses by index in an `IntStream.range`. For binary Unions (2 children), the inner `IntStream.range(1, 2)` loop accesses index 1 only once per column position. The LinkedList issue is O(C) per access, where C = number of children. For typical binary Unions, C = 2, making the linked-list access effectively O(1) in practice.

With 259 Union nodes and ~96 columns per Union, total work is:
```
259 Unions × 96 cols × O(2) LinkedList access = ~49,824 operations per CLL traversal
```

This is negligible compared to F-5 and F-4. The fix (change to ArrayList) is trivially correct and cheap.

**Rating at production scale**: Marginal — fix is one-line but impact is negligible compared to other issues.

---

### F-8: `IcebergMergeIntoVisitor.isDefinedAt` — List Alloc + `getCanonicalName()` Per Plan Node

**Source**: Group 8 — Severity originally: MEDIUM

`isDefinedAt` allocates a new `Arrays.asList()` and calls `getCanonicalName()` for every plan node in the CLL `plan.foreach` traversal.

```
6,778 nodes × 4 CLL invocations × 2 (CLL fires twice: ExpressionDependencyCollector + InputFieldsCollector)
= 54,224 allocations of ArrayList + getCanonicalName() calls per exec
```

However, the plan has only 1 MergeInto node, so this only matters if Iceberg is on the classpath. If Iceberg is not present, `Class.forName(MERGE_ROWS_CLASS_NAME)` fails and the visitor short-circuits. With Iceberg present, the `getCanonicalName()` string construction on 54,224 nodes per exec is modest but real.

**Rating at production scale**: Significant (if Iceberg is on classpath) / Marginal (if not)

---

### F-9: `UnknownEntryFacetListener` Serializes Leaf Plans on Every Event

**Source**: Group 1 — Severity originally: MEDIUM

`UnknownEntryFacetListener.build()` calls `root.collectLeaves()` (full O(N) DFS) and serializes each unvisited leaf via `planSerializer.serialize(x)` using reflection-heavy Jackson serialization.

With 310 leaf nodes and the `spark_unknown` facet enabled (default):
```
310 leaf serializations per event × 15 events = 4,650 leaf serializations/exec
```

Each serialization runs `MethodUtils.invokeMethod(objectMapper, "writeValueAsString", x)` — a reflective call that serializes the full leaf node (including schema data). For `HadoopFsRelation` or `DataSourceV2Relation` nodes, the serialized form can be hundreds of bytes. This is significant memory allocation.

**Rating at production scale**: Significant (more leaves than baseline amplifies this finding)

---

## Part 2: Dataset-API Node Specifics (SerializeFromObject, MapElements, etc.)

### F-10: CLL Handling of Dataset[T] Encoder Nodes

**Investigation**: Examining `InputFieldsCollector.discoverInputsFromNode`, `ExpressionDependencyCollector.collectFromOperator`, and `VisitorFactory.operatorVisitors()`.

**Finding**: The 3,161 Dataset-API nodes (SerializeFromObject, DeserializeToObject, MapElements, AppendColumns, MapGroups) are **not handled by any CLL operator visitor**. None of the 12 operator visitors in `VisitorFactory.operatorVisitors()` has an `isDefinedAt` match for these types:
- `ProjectVisitor`: `instanceof Project` — no match
- `FilterVisitor`: `instanceof Filter` — no match
- `AggregateVisitor`: `instanceof Aggregate` — no match
- etc.

`InputFieldsCollector.discoverInputsFromNode` for a `SerializeFromObject` or `MapElements` node falls through all instanceof checks (they are not `LeafNode` instances — they have children), returns empty `datasetIdentifiers`, and calls `extractInternalInputs` with an empty list, which is a no-op.

**Implication for column lineage**: Column lineage is **broken** across Dataset[T] API boundaries. The encoder expression trees in SerializeFromObject (which encode the Scala/Java object to a Spark internal row) produce output ExprIds that are disconnected from the ExprIds of the input columns. The CLL engine cannot trace through these object codec nodes.

This means that for a pipeline like:
```
Relation → DeserializeToObject → MapElements → SerializeFromObject → Project → ... → InsertInto
```
The lineage graph is severed at each encode/decode boundary. The final InsertInto's output columns cannot be traced back to the source Relation's columns.

**Performance implication**: Despite the CLL engine traversing all 6,778 nodes, the 3,161 Dataset-API nodes contribute **zero useful lineage information** and are pure overhead.

**Cost of traversing Dataset-API nodes in CLL**:
```
3,161 nodes × 12 operator visitor isDefinedAt checks × 4 CLL traversals × 2 passes (ExprDep + InputFields)
= 3,161 × 12 × 8 = 303,456 wasted isDefinedAt checks per exec
```

Beyond the wasted checks, the `InputFieldsCollector` also converts `plan.children()` from Scala to Java for each of these 3,161 nodes (via `ScalaConversionUtils.fromSeq(plan.children())`), which allocates a Java list on each call:
```
3,161 × ScalaConversionUtils.fromSeq() × 4 CLL invocations = ~12,644 unnecessary list allocations/exec
```

**Rating at production scale**: The performance impact is Significant (wasted traversal). The correctness impact (broken lineage) is Critical — but that is a design limitation, not a bug.

---

### F-11: `OutputFieldsCollector` on Dataset-API Nodes

`OutputFieldsCollector.getOutputExpressionsFromRoot` calls `plan.output()` for every node and casts to `Attribute`. For a `SerializeFromObject`, `output()` returns the serialized attributes (the column names and types for the resulting Row). These are passed to `addOutput()`, which scans the 790-entry schema map to find a match. Many of these 29,850 total output column references come from intermediate Dataset-API nodes whose columns are not in the final schema, resulting in cache misses in `addOutput` (the `putIfAbsent` silently does nothing):

```
Wasted addOutput() calls from Dataset-API nodes:
~3,161 nodes × avg 9 output cols each = ~28,449 addOutput() calls that produce no result
Each scans 790 schema entries → ~22.5M wasted string comparisons just from Dataset-API nodes
```

**Rating at production scale**: Critical (subsumed under F-4)

---

## Part 3: Non-Plan-Size-Scaling Findings (Per-Leaf or Per-Execution)

### F-12: `catalog.loadTable()` Per Iceberg/Delta Relation Per Event

**Source**: Groups 3, 4, 5 — Severity originally: HIGH

**Scale with leaf count**: `IcebergHandler.getIcebergTable()` and `DeltaHandler.loadTable()` are called per DataSourceV2Relation leaf node that matches the handler. Of the 310 total leaf Relation nodes, only those backed by Iceberg or Delta trigger these paths.

In a typical Dataset[T] pipeline that reads from tables (e.g., Parquet/Iceberg files), many of the 310 Relations may be `DataSourceV2Relation` or `LogicalRelation`. If even 50 of the 310 are Iceberg relations:

- `loadTable()` called twice per Iceberg relation (once for identifier, once for version): 2 HTTP calls per relation
- `CatalogUtils3.getHandlers()` called 4 times per relation: 4 × 6 handler allocations = 24 allocations per relation
- 50 relations × 15 events: only 1 event type (SQL execution end) triggers the full extraction path

Per-execution cost (assuming 50 Iceberg relations):
```
50 relations × 2 loadTable() calls × 1 extraction event = 100 HTTP round-trips/exec
```

Each HTTP round-trip to a REST catalog is 5–50ms. At 10ms average:
```
100 × 10ms = 1,000ms of network-blocking time per execution
```

The `CatalogUtils3.getHandlers()` rebuild and class-loading occur 4 times per relation × 50 relations × 15 events for which the handler guard is evaluated:
```
50 × 4 × 15 = 3,000 handler-list rebuilds/exec, each allocating 6 handler objects
```

**Rating at production scale**: CRITICAL (network blocking dominates)

---

### F-13: GCP Metadata Service HTTP Calls

**Source**: Group 13 — Severity originally: HIGH

This finding is per-event, not per-node. With 15 events/exec and 4–8 HTTP calls per event (from `GcpRunFacetBuilder` + `GcpJobFacetBuilder`):
```
15 events × 8 HTTP calls = 120 HTTP calls to http://169.254.169.254/exec
```

Each call has at minimum network RTT overhead. This does not scale with plan size and is constant regardless of the 6,778 nodes. However, it stacks on top of the already-high node-traversal costs.

**Rating at production scale**: Critical (unchanged from sub-audit — unique to GCP Dataproc deployments)

---

### F-14: HMS Thrift RPCs from Hive Path Resolution

**Source**: Groups 2, 9 — Severity originally: HIGH

`PathUtils.fromCatalogTable` triggers `SessionCatalog.defaultTablePath()` → two HMS Thrift RPCs (`databaseExists` + `getDatabase`) when the table has no explicit `storage.locationUri`. `InsertIntoHiveTableVisitor` calls this up to 5 times for the same table.

For Dataset[T] pipelines reading Hive-managed tables: if 100 of the 310 Relations are Hive-managed tables without explicit locationUri:
```
100 relations × 2 HMS RPCs per PathUtils.fromCatalogTable() = 200 HMS RPCs per extraction event
```

With `InsertIntoHiveTableVisitor`'s 5× duplication, the single write target could add another 8 (5 × 2 - 2 deduplication) RPCs.

**Rating at production scale**: Critical for Hive-backed deployments

---

### F-15: `SparkPropertyFacetBuilder` Full Config Scan Per Event

**Source**: Group 12 — Severity originally: MEDIUM

`SparkConf.getAll()` copies the entire config array on every event, then scans it to find 2 allowed keys. This does not scale with plan size.

```
15 events × O(all_config_entries) scan = fixed overhead per exec
```

For a cluster with 500 config entries, this is 7,500 config-entry scans per exec — negligible in absolute time (~microseconds) but wasteful.

**Rating at production scale**: Marginal

---

### F-16: `LogicalPlanRunFacetBuilder` and `DebugRunFacetBuilderDelegate`

**Source**: Group 12 — Severity originally: HIGH

`plan.toJSON()` serializes the full 6,778-node plan to JSON. `DebugRunFacetBuilderDelegate.scanLogicalPlan()` calls `node.toString()` (= `treeString()`) for every node, generating an O(N²) string construction where N = 6,778.

At N = 6,778:
```
O(N²) = 6,778² ≈ 45.9M operations for treeString() across all nodes
```

This would produce multi-megabyte string output. Both facets are disabled by default.

**If enabled at production plan size**:
- `LogicalPlanRunFacetBuilder`: Would produce JSON potentially 100+ MB in size (6,778 nodes × schema data per node)
- `DebugRunFacetBuilderDelegate`: Would generate O(N²) strings, likely OOM or extreme GC

**Rating at production scale**: N/A (disabled by default) / Critical if enabled (would likely OOM or timeout)

---

### F-17: `DatabricksEnvironmentFacetBuilder` DBFS Mount Enumeration

**Source**: Group 13 — Severity originally: MEDIUM

Per-event RPC to Databricks DBFS service. Does not scale with plan size. 15 events × DBFS RPC per event on Databricks clusters.

**Rating at production scale**: Significant for Databricks deployments (unchanged)

---

## Part 4: Interaction Effects and Compounding Costs

### I-1: CLL + Dataset-API Nodes = Broken Lineage at Massive Overhead

The 3,161 Dataset-API nodes cause CLL to:
1. Traverse them (O(N) cost across 4 passes, N = 3,161)
2. Produce ExprId mappings that go nowhere (encoder output ExprIds not in the final schema)
3. Attempt and fail `addOutput` schema scans for all their output columns
4. Leave the column lineage disconnected at every encode/decode boundary

The net result: CLL spends significant resources on these 3,161 nodes, produces broken lineage, and emits an empty or misleading `ColumnLineageDatasetFacet`. If the facet is empty (because no lineage traces from root output to any input), CLL returns early (`facet.getFields().getAdditionalProperties().isEmpty()` → return `Optional.empty()`). This may mean CLL's work is entirely wasted for this plan.

**Recommendation**: Add `instanceof` guards for SerializeFromObject, DeserializeToObject, MapElements, MapGroups, AppendColumns at the top of `ExpressionDependencyCollector.collectFromOperator` and `InputFieldsCollector.discoverInputsFromNode` to short-circuit without iterating any visitors. This turns wasted work into a single cheap instanceof check.

---

### I-2: Reflection Cost Compounds Across All Traversals

The uncached reflection in `isDefinedAt` (F-1) compounds with the multiple traversals (F-2). Each of the 4 traversal passes calls `isDefinedAt` for every node × every visitor. The 4 CLL traversals also call the plan-level `isDefinedAt`/`isDefinedAtLogicalPlan` for each of the ~23 registered builders. The total reflection call count across all traversals is:

```
Plan-level reflection (visitors, F-1): 9,356,280 calls/exec
CLL operator isDefinedAt (12 visitors × 6,778 × 4 passes × 2 CLL runs × 4 events):
  12 × 6,778 × 8 × 4 = 2,605,824 instanceof checks (but these use direct instanceof — fast)
```

The CLL operator visitors use direct `instanceof` (clean) — only the plan-level visitors use reflection (broken). So reflection cost is concentrated in the 9.3M calls from F-1.

---

### I-3: `addOutput` Schema Scan × 259 Unions Amplification

With 259 Union nodes each exposing ~96 output columns, the `addOutput` calls include:
```
259 × 96 = 24,864 addOutput() calls just from Union nodes
```

These are in addition to Project (1,258 × 96 = 120,768), Filter (858 × 96 = 82,368), and other nodes. The schema scan amplification is severe because Union nodes expose the full column set of all merged branches — but these Union output ExprIds are valid and map to the final schema.

---

### I-4: BFS Depth 134 × 259 Unions = Maximal Dependency Chain Length

With max plan depth of 134 and 259 Union nodes, a column's dependency chain can traverse up to 259 Union nodes sequentially (each adds one hop) plus additional Project/Filter/Aggregate hops. With D ≈ 400 (upper estimate), the BFS without `HashSet` is O(D²) = O(160,000) per output column. With 790 output columns:

```
Without fix: 790 × 160,000 = 126.4M comparisons per findDependentInputs sweep
With HashSet: 790 × 400 = 316,000 comparisons — a 400× improvement
```

The depth-134 tree also means `InputFieldsCollector.collect` recurses 134 levels deep via Java stack frames. At default JVM stack depth (512 frames), this is not an overflow risk, but it does cause deep recursion.

---

## Part 5: Revised Priority Order Table

| Priority | Finding | Source Group | Severity at Production Scale | Estimated Cost/Exec | Fix Complexity |
|---|---|---|---|---|---|
| 1 | F-5: BFS O(D²) visited-set in `findDependentInputs` | G6, G7, G8 | Critical | ~505M ops | Low (change LinkedList to HashSet) |
| 2 | F-4: `addOutput` O(S) schema scan (29,850 calls × 790-entry scan) | G6, G7 | Critical | ~94M comparisons | Low (add Map index in constructor) |
| 3 | F-1: Uncached reflection in `isDefinedAt` hot path | G1 | Critical | ~700ms–1.4s | Low (cache Class in constructor) |
| 4 | F-3: ExpressionTraverser VisitorFactory allocation storm | G6, G7 | Critical | ~61M object allocs | Low (reuse VisitorFactory) |
| 5 | F-10/F-11: CLL wastes full traversal on 3,161 Dataset-API nodes with no result | G6, G8 | Critical (correctness + perf) | ~22.5M wasted comparisons | Medium (add instanceof guards) |
| 6 | F-12: `catalog.loadTable()` per Iceberg/Delta relation per event | G3, G4, G5 | Critical (network) | ~1s network I/O | Medium (pass table through call chain) |
| 7 | F-2: 4 independent full plan traversals per CLL invocation | G6 | Critical | O(4 × 6,778 × visitors) | Medium (unify traversal) |
| 8 | F-14: HMS Thrift RPCs from path resolution | G2, G9 | Critical (Hive only) | ~200ms network I/O | Medium (cache namespace metadata) |
| 9 | F-13: GCP metadata service HTTP calls per event | G13 | Critical (GCP only) | ~120 HTTP calls | Low (cache static fields) |
| 10 | F-6: `datasetDependencyInputs` repeated BFS (depends on F-5) | G6 | Significant | Subsumed in F-5 | Low (memoize BFS) |
| 11 | F-9: `UnknownEntryFacetListener` serializes 310 leaf nodes per event | G1 | Significant | 4,650 reflective serializations | Low (cache per execution ID) |
| 12 | F-17: Databricks DBFS mount enumeration per event | G13 | Significant (Databricks) | Per-event DBFS RPC | Low (cache after first call) |
| 13 | F-8: IcebergMergeIntoVisitor.isDefinedAt allocates per 6,778 nodes | G8 | Significant | ~54K allocs/exec | Low (cache Class references) |
| 14 | F-15: SparkPropertyFacetBuilder full config scan per event | G12 | Marginal | ~7,500 map entries scanned | Low (direct key lookup) |
| 15 | F-7: UnionVisitor LinkedList indexed access | G8 | Marginal | Negligible | Trivial |
| 16 | F-16: LogicalPlan/Debug facets | G12 | N/A (disabled) | OOM if enabled | Medium |

---

## Part 6: Findings That Do NOT Scale with Plan Size

The following findings are fixed costs per execution regardless of the 6,778-node plan:

- **GCP metadata HTTP calls** (F-13): Fixed 120 HTTP calls/exec regardless of plan size
- **HMS path resolution RPCs** (F-14): Scales with leaf count (310), not total nodes
- **Iceberg/Delta `loadTable()` RPCs** (F-12): Scales with matching-leaf count, not total nodes
- **Databricks DBFS mount enumeration** (F-17): Fixed per job event
- **SparkPropertyFacetBuilder config scan** (F-15): Fixed per event
- **`DatabricksEnvironmentFacetBuilder` env reads**: Fixed per event
- **JNI SQL parsing** (Group 2 JDBC finding): Per JDBC relation, not per total nodes
- **Kafka/streaming reflection** (Group 10): Per matching Kafka relation only
- **Hive DDL catalog calls** (Group 1, `catalogTableFor`): Only for DDL commands (none in this pipeline per the node type distribution)

---

## Part 7: Plan-Specific Observations

### The plan has 1 InsertIntoHadoopFsRelationCommand (root output)

This is the only output node. Most output builder visitors will fail their `isDefinedAt` at the root and return immediately. The searchDependencies traversal in `LogicalRelationDatasetBuilder(searchDependencies=true)` will still walk all 6,778 nodes looking for `LogicalRelation` leaf nodes to use as inputs.

### 174 SubqueryAlias nodes trigger SubqueryAliasInputDatasetBuilder

Each SubqueryAlias triggers `SubqueryAliasInputDatasetBuilder.apply()`, which creates its own local visitor list including `new LogicalRelationDatasetBuilder(context, DatasetFactory.input(context), true)`. This means the searchDependencies plan walk is triggered **once per SubqueryAlias** — potentially 174 additional full-plan traversals beyond the base traversal:

```
174 SubqueryAlias nodes × 6,778 nodes per traversal × 15 events
= 17,691,540 extra node visits/exec just from SubqueryAliasInputDatasetBuilder
```

This is an extremely significant finding unique to this plan's structure. Verify in `SubqueryAliasInputDatasetBuilder.java`:
```java
// SubqueryAliasInputDatasetBuilder.java:36
.add(new LogicalRelationDatasetBuilder(context, DatasetFactory.input(context), true))
```

If `SubqueryAliasInputDatasetBuilder` is called for every SubqueryAlias node in the plan (via `searchDependencies=true` logic), each triggers another full plan traversal. At 174 SubqueryAlias nodes and 15 events:

```
174 × 6,778 × 15 = 17,691,540 additional node visits/exec
```

This is larger than the base traversal cost and was not estimated by any sub-audit group.

**Rating**: CRITICAL — This may actually be the single largest CPU cost in the production plan.

### 259 Union nodes and CLL correctness

`UnionVisitor.apply()` maps each Union child's column ExprIds to the first child's ExprIds. With 259 Unions, the dependency graph is a long chain of ExprId mappings. The BFS in `findDependentInputs` must traverse this entire chain for each output column — this is exactly the D ≈ 400 estimate used in F-5 above.

### 175 Repartition and 60 Deduplicate nodes

These are `UnaryNode` subtypes with no dedicated CLL operator visitor. They are transparent to CLL — their output ExprIds equal their input ExprIds (passthrough). The CLL engine correctly handles these by not registering any dependencies, allowing the BFS to trace through them implicitly via Spark's expression ID reuse. No special handling is needed. These 235 nodes are pure traversal overhead in CLL (12 operator-visitor `isDefinedAt` checks per node, all returning false).

---

## Summary

At 6,778 nodes (34× the baseline), the OpenLineage Spark integration faces qualitatively different performance characteristics than sub-audits estimated:

1. **CLL is the dominant cost** at this scale, not the plan-level visitor reflection. The combination of 4 full traversals, O(D²) BFS, and O(S) schema scan per output registration makes CLL cost O(N × D² × W) — potentially billions of operations per execution. For this specific plan, CLL likely produces **empty or broken lineage** due to the Dataset[T] encoder nodes severing all expression chains, making the entire CLL cost wasted.

2. **Uncached `isDefinedAt` reflection** is the next largest CPU cost at ~700ms–1.4s/exec, compounded by the discovery that SubqueryAlias handling may trigger up to 174 additional full-plan traversals beyond what any sub-audit estimated.

3. **Network-blocking costs** (Iceberg `loadTable()`, HMS RPCs, GCP metadata, DBFS) are environment-dependent but are fixed overhead independent of plan size.

4. **The most impactful quick fixes** (in order of complexity vs. impact):
   - HashSet in `findDependentInputs` (1 line change, 400× CLL improvement)
   - Schema name-to-field Map in `ColumnLevelLineageBuilder` constructor (5 lines, eliminates O(S) scans)
   - Cache `Class<?>` in visitor constructors (5 lines per visitor, eliminates 9.3M reflection calls/exec)
   - Reuse VisitorFactory singleton in ExpressionTraverser (1 line, eliminates 61M allocations/exec)
   - Add `instanceof` guard for Dataset-API nodes in CLL collectors (10 lines, eliminates wasted traversal of 3,161 nodes)
