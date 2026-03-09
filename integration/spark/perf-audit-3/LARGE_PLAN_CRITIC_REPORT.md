# Critic Review of LARGE_PLAN_AUDIT_REPORT.md

Review methodology: all claims were verified against the actual source files at
`~/perf-audit/OpenLineage/integration/spark/` and `~/perf-audit/spark/`.
Every citation refers to an absolute path and line number read during this review.

---

## Confirmed Findings

### F-1 — Uncached reflection in `isDefinedAt` (CONFIRMED, but arithmetic needs qualification)

The reflection path is real and verified.

`QueryPlanVisitor.isDefinedAt(LogicalPlan x)` calls `getClass().getGenericSuperclass()` and
`getActualTypeArguments()` on every invocation with no caching.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/QueryPlanVisitor.java`, lines 96–110.

The same reflective pattern exists in `AbstractPartial.isDefinedAt()`:

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractPartial.java`, lines 25–39.

And in `AbstractQueryPlanDatasetBuilder.isDefinedAtLogicalPlan()`:

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractQueryPlanDatasetBuilder.java`, lines 150–163.

The `PlanUtils.merge()` double-evaluation is also confirmed: in `PlanUtils.merge().isDefinedAt(x)`
(line 62) the function calls `PlanUtils.safeIsDefinedAt(pfn, x)` to decide if it is defined, then
calls `PlanUtils.safeIsDefinedAt(pfn, x)` again inside `apply()` (line 75) to filter before applying.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/PlanUtils.java`, lines 57–103.

**Verdict**: The reflection problem is real and severe. The precise multiplier of "~23 visitors × 4
traversals" is approximately correct in magnitude. Section below corrects the subtraction for
SearchDependencies-only vs. non-SearchDependencies traversals.

### F-5 — BFS O(D²) visited-set in `findDependentInputs` (CONFIRMED)

`ColumnLevelLineageBuilder.findDependentInputs()` uses `List<Dependency> dependentInputs = new
LinkedList<>()` as its visited set and filters at line 298 with
`!dependentInputs.contains(dependency)`. `LinkedList.contains()` is O(D). This runs once per
candidate in each BFS round, giving O(D²) total.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java`, lines 284–306.

The `Dependency.equals()` is correctly implemented at lines 32–40 of the same file — the dedup is
logically correct but structurally O(D²).

The claim that D ≈ 400 for a pipeline with 259 Union nodes is plausible but unverifiable without
running the code. The BFS traverses the **exprDependencies HashMap** (in-memory), not the logical
plan tree. UnionVisitor adds one dependency hop per Union node per column position (see below under
F-7). With 259 sequential Unions each adding a hop, a dependency chain of depth 259 is realistic
and D ≈ 400 is a reasonable estimate.

**Verdict**: Confirmed correct. The fix (use a `HashSet<Dependency>`) is valid because `Dependency`
implements `hashCode()` and `equals()` correctly (lines 37–42 same file).

### F-7 — `UnionVisitor` LinkedList indexed access (CONFIRMED, but severity correctly rated marginal)

`UnionVisitor.apply()` stores child attribute lists in a `LinkedList<ArrayList<Attribute>>`
(`childrenAttributes` at line 44) and then accesses by index via
`childrenAttributes.get(childIndex)` inside `IntStream.range(1, children.size())` (line 58).

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/operator/UnionVisitor.java`, lines 40–62.

For binary Unions (children.size()==2), `childrenAttributes.get(1)` is called once per column
position — O(2) LinkedList traversal, effectively negligible. The report correctly rates this
marginal.

**Note**: UnionVisitor also triggers `ExpressionTraverser.of(attr, firstExpr, builder).traverse()`
for each child attribute at each position (line 59). Each `attr` is an `AttributeReference` (leaf
node), so `isLeafNode()` returns true immediately and a new dependency is added. This creates one
dependency hop per Union per column, which IS the mechanism behind D ≈ 259 Union hops.

**Verdict**: Confirmed. Severity rating of "marginal" is correct.

### F-8 — `IcebergMergeIntoVisitor.isDefinedAt` allocates per plan node (CONFIRMED)

`IcebergMergeIntoVisitor.isDefinedAt()` calls `Arrays.asList(MERGE_INTO_CLASS_NAME,
MERGE_ROWS_CLASS_NAME)` (allocating a new list each time) and
`operator.getClass().getCanonicalName()` (string construction) for every plan node during CLL
traversal.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/operator/IcebergMergeIntoVisitor.java`, lines 40–43.

The allocation happens every time `ExpressionDependencyCollector.collectFromOperator()` filters the
12 operator visitors (line 31–33 of `ExpressionDependencyCollector.java`). The report's allocation
count is approximately correct.

**Verdict**: Confirmed.

### F-9 — `UnknownEntryFacetListener` serializes leaf nodes (CONFIRMED in mechanism)

`asQueryPlanVisitor()`'s inner `apply(LogicalPlan x)` calls
`unknownEntryFacetListener.accept(x)` at line 99–101.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractQueryPlanDatasetBuilder.java`, lines 98–110.

This fires for every node that passes `isDefinedAt()` in any of the `searchDependencies=true`
traversals. The 310-leaf-serialization estimate is plausible but the actual count depends on which
nodes pass `isDefinedAt()` for the listener's wrapping visitor. The report's direction is correct.

### F-10 / F-11 — CLL traverses Dataset-API nodes with no result (CONFIRMED)

Verified in `InputFieldsCollector.extractDatasetIdentifier()`: the method has explicit `instanceof`
checks for `DataSourceV2Relation`, `DataSourceV2ScanRelation`, `HiveTableRelation`,
`LogicalRelation`, `InMemoryRelation`, `OneRowRelation`, `LocalRelation`, `ExternalRDD`,
`LogicalRDD`, and `LeafNode`. None of `SerializeFromObject`, `DeserializeToObject`, `MapElements`,
`MapGroups`, or `AppendColumns` appear in this list.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/InputFieldsCollector.java`, lines 106–150.

These Dataset-API nodes are `UnaryNode` subtypes (not `LeafNode`), confirmed in Spark source:

Source: `/home/bits/perf-audit/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/object.scala`, lines 71, 79–82, 92–94, 123–124, 169, 223–229, 258, 368–374.

The CLL engine traverses all their children recursively (lines 64–67 of `InputFieldsCollector.java`)
and fires all 12 operator-visitor `isDefinedAt()` checks via `ExpressionDependencyCollector`
(line 31 of `ExpressionDependencyCollector.java`) for each one — all returning false. This is
confirmed wasted traversal.

### F-13, F-17 — GCP and Databricks costs do not scale with plan size (CONFIRMED)

Report correctly identifies these as per-event fixed costs. GCP metadata calls are per-event; DBFS
mount enumeration is per-job-event. Neither scales with 6,778 nodes. The report's classification
and direction are correct.

### CLL schema size limit assessment (CONFIRMED)

`isSchemaExceedsLimit()` checks `schemaFacet.getFields().size() > schemaSizeLimit` where
`schemaFacet` is the output schema of the final plan (passed in from the output dataset's schema),
not any aggregate of intermediate columns.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ColumnLevelLineageUtils.java`, lines 90–107.

The `ColumnLevelLineageBuilder` is constructed with that same `schemaFacet`. For a target table
with ~96 output fields, `schemaFacet.getFields().size()` ≈ 96 — well under the 1,000 default
limit. The report's conclusion that CLL will run on every event for this plan is correct.

---

## Overstated / Incorrect Claims

### INCORRECT — F-0 (Part 7): The "174 SubqueryAlias × 6,778 nodes = 17.7M extra node visits" claim

This is the most significant factual error in the report.

**The claim**: Each of the 174 SubqueryAlias nodes, when processed by
`SubqueryAliasInputDatasetBuilder.apply()`, instantiates a `LogicalRelationDatasetBuilder
(searchDependencies=true)` in its local builder list and triggers a full plan traversal via that
builder's `collect()`.

**What the code actually does**:

`SubqueryAliasInputDatasetBuilder.apply(event, SubqueryAlias x)` calls `delegate(event, x.child())`
at line 55, which calls `delegate(Collections.emptyList(), inputDatasetBuilders, event).applyOrElse(node, ...)`.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/SubqueryAliasInputDatasetBuilder.java`, lines 49–64.

The `delegate()` method (inherited from `AbstractQueryPlanDatasetBuilder`) at lines 123–141
wraps each builder in `asQueryPlanVisitor(event)` and creates a `PlanUtils.merge()` partial
function. This merged function is then called with `.applyOrElse(node, ...)` where `node` is the
single child of the SubqueryAlias — **not the full optimized plan**.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractQueryPlanDatasetBuilder.java`, lines 123–141.

When a builder is wrapped via `asQueryPlanVisitor()`, its `apply(LogicalPlan x)` method calls
`builder.apply(event, (P) x)` (line 105). This is NOT the same as `apply(T event)` (line 66)
which is the entry point that calls `qe.optimizedPlan().collect(visitor)`. The `searchDependencies`
flag only governs the `apply(T event)` top-level path. When a builder is used inside a `delegate()`
call, it acts as a simple node visitor — it processes the single node handed to it. No full-plan
`collect()` is triggered.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractQueryPlanDatasetBuilder.java`, lines 66–85 and 98–116.

**Corrected behavior**: `SubqueryAliasInputDatasetBuilder` does **one** full-plan traversal (from
its own outer `searchDependencies=true` `collect()` call) to find all 174 SubqueryAlias nodes. For
each one, it applies 4 local visitors to a **single node** (the child). The claimed 17,691,540
extra node visits does not occur. The additional cost is:
```
174 SubqueryAlias nodes × 4 local visitor isDefinedAt checks × 1 (single node) = 696 checks
```
This is negligible, not critical.

The "new finding" labeled as potentially "the single largest CPU cost in the production plan" at the
end of Part 7 is not supported by the code.

**The code comment at line 54 of SubqueryAliasInputDatasetBuilder.java** confirms the intent:
`// this should not run query visitors again`

### OVERSTATED — F-4: "29,850 addOutput() calls × 790-field schema scan"

**The claim**: `OutputFieldsCollector.collect()` calls `addOutput()` for each node's output
attributes across all 6,778 nodes, giving 29,850 total `addOutput()` calls, each scanning 790
schema entries.

**What the code actually does**:

`OutputFieldsCollector.collect()` at lines 24–35 processes the **root plan node** only:
```java
getOutputExpressionsFromRoot(plan).stream()
    .forEach(expr -> context.getBuilder().addOutput(expr.exprId(), expr.name()));
```
It recurses into children **only if** `!context.getBuilder().hasOutputs()` (i.e., only if no
outputs were found at the current level).

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/OutputFieldsCollector.java`, lines 24–35.

For an `InsertIntoHadoopFsRelationCommand` root, `output()` returns an empty sequence (insert
commands don't return rows). So `OutputFieldsCollector` does recurse down the tree. However, once
the first child level that has non-empty output is found (the first Project, Aggregate, or Union
child), the recursion stops for that branch. It does NOT visit all 6,778 nodes.

Furthermore, `addOutput()` scans `schema.getFields()` — the OUTPUT schema of the final dataset (the
target table, ~96 fields), not 790 unique output ExprIds from across the entire plan. The schema
passed to `ColumnLevelLineageBuilder` is the `schemaFacet` of the output dataset:

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ColumnLevelLineageUtils.java`, line 56.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java`, lines 91–96.

The `addOutput()` scan is O(output_schema_size), not O(790). For a table with ~96 output fields,
each `addOutput()` call scans ~96 fields, not 790. The 790-field number is the count of unique
ExprIds across the whole plan — not the output schema size.

**Corrected estimate**: `addOutput()` is called for the output expressions of the nodes visited
during OutputFieldsCollector's partial DFS. In the worst case (INSERT command with empty root
output), this might reach several hundred nodes deep before finding outputs, but it stops early.
The actual call count is far less than 29,850. And each call scans ~96 schema fields, not 790.

The finding is still valid (O(S) scan should be a Map lookup), but the magnitude is overstated by
approximately two orders of magnitude. The real cost is significant but not critical.

### OVERSTATED — F-2: "4 full O(N) CLL traversals"

**The claim**: `buildColumnLineageDatasetFacet` triggers 4 full O(N) traversals.

**What the code actually does**:

From `ColumnLevelLineageUtils.collectInputsAndExpressionDependencies()`:
1. `ExpressionDependencyCollector.collect(context, plan)` — calls `plan.foreach(...)` which IS a full traversal of all 6,778 nodes. (line 22 of ExpressionDependencyCollector.java)
2. `InputFieldsCollector.collect(context, plan)` — recursive DFS visiting all nodes and their children. (lines 52–68 of InputFieldsCollector.java)
3. `plan.foreach(node -> { if (node instanceof InMemoryRelation) ... })` — another full traversal. (lines 131–159 of ColumnLevelLineageUtils.java)

And separately:
4. `OutputFieldsCollector.collect(context, plan)` — partial DFS, **not** necessarily a full traversal. It stops once outputs are found at a subtree level.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ColumnLevelLineageUtils.java`, lines 61–62 and 125–160.

So there are 3 confirmed full-plan traversals and 1 partial DFS. The report's "4 full O(N)
traversals" is approximately correct but overstates the OutputFieldsCollector contribution.

Additionally, the CLL invocation count arithmetic in the report is inconsistent. The report first
says CLL fires ~2 times/exec, then revises to ~4 times. The actual count depends on how many output
datasets are produced and how many qualifying events occur. CLL is called once per output dataset
per `buildOutputDatasets()` call (line 370 of `OpenLineageRunEventBuilder.java`). If there is 1
output dataset and 4 qualifying events, CLL runs 4 times. This is speculative without runtime data.

### PARTIALLY INCORRECT — F-3: ExpressionTraverser VisitorFactory allocation

**The claim**: Each `copyFor()` call creates "1 VisitorFactory + 18 visitor objects."

**What the code actually does**:

`ExpressionTraverser.of()` at line 84 creates `new VisitorFactory()`. `VisitorFactory` instantiates:
- 12 operator visitors (line 34–47 of VisitorFactory.java)
- 6 expression visitors (line 49–57 of VisitorFactory.java)
= 18 total objects per VisitorFactory instantiation

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/VisitorFactory.java`, lines 32–58.

However, `ExpressionTraverser.traverse()` only uses `visitorFactory.expressionVisitors()` (line 109
of ExpressionTraverser.java). The 12 operator visitors are never used by ExpressionTraverser. They
are created but immediately garbage-collected. So the waste is real — 12 unnecessary operator
visitor instantiations per `copyFor()` call — but calling this "18 visitor objects per call" is
technically correct (they are all allocated) even if 12 are wasted.

**Note on operator-level VisitorFactory**: `ExpressionDependencyCollector` uses a
`private static final VisitorFactory visitorFactory = new VisitorFactory()` (line 19 of
`ExpressionDependencyCollector.java`), so the operator-visitor VisitorFactory IS a singleton at the
collector level. Only the per-ExpressionTraverser VisitorFactory allocations are the problem.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ExpressionDependencyCollector.java`, line 19.

**The arithmetic for F-3** is speculative. The report estimates "1,258 Project nodes × 96 avg
expressions × 7 expr nodes per expression × 18 visitor objects = 15,226,416 visitor object
allocations." This chain of multiplications is reasonable in structure but the "96 avg expressions"
per Project and "7 expr nodes per expression" are unverified assumptions. The direction is correct
but the exact numbers cannot be confirmed from static analysis.

### OVERSTATED — F-12: CatalogUtils3.getHandlers() per "310 Relations"

**The claim**: "310 Relations × 4 calls × 15 events = 18,600 handler-list rebuilds/exec."

**What the code actually does**:

`CatalogUtils3.getHandlers()` is called from:
- `CatalogUtils3.getDatasetIdentifier()` (line 48)
- `CatalogUtils3.getCatalogHandler()` (line 76) — called from `getStorageDatasetFacet()` and `getCatalogDatasetFacet()`

These methods are only called for **DataSourceV2Relation** nodes, specifically in:
- `DataSourceV2Relation` processing paths in various handlers
- `LogicalRelationDatasetBuilder.addCatalogAndStorageFacets()` (only for Hive-catalog LogicalRelation nodes with a catalog name)

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/CatalogUtils3.java`, lines 43–77.

The Dataset[T] pipeline in this audit has 310 Relation nodes of mixed types: `LogicalRelation`
(HadoopFsRelation for file reads), `DataSourceV2Relation`, `LogicalRDD`, etc. Only the
`DataSourceV2Relation` subset triggers `CatalogUtils3.getHandlers()`. For a Dataset[T] pipeline
reading from in-memory data or file-based sources without V2 catalog, few or none of the 310 nodes
may be `DataSourceV2Relation`. The "310 Relations × 4 calls" multiplier is not justified.

**The report should have flagged this uncertainty** rather than assuming all 310 trigger CatalogUtils3.

---

## Understated or Missing Issues

### UNDERSTATED — `OutputFieldsCollector` recursion in INSERT plans

When the root is `InsertIntoHadoopFsRelationCommand` (which has empty `output()`),
`OutputFieldsCollector.collect()` does recursively descend into all children. For a plan with
depth 134 and many Union/Project nodes, this recursive DFS may visit hundreds to thousands of nodes
before finding outputs. The `addOutput()` cost is understated in F-4's correction above, but the
traversal depth is also a hidden cost. Each recursive level allocates Scala-to-Java collection
conversions via `ScalaConversionUtils.fromSeq(plan.children())`.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/OutputFieldsCollector.java`, lines 31–34.

### MISSING — `PlanUtils.merge()` applies ALL matching visitors, not just the first

Looking at `PlanUtils.merge().apply(T x)` at lines 73–97 of `PlanUtils.java`: it does NOT
short-circuit at the first matching visitor. It calls `.filter(pfn -> PlanUtils.safeIsDefinedAt(pfn, x))`
and then `.map(pfn -> pfn.apply(x))` — collecting results from ALL matching visitors. This means
for a node that matches multiple visitors, each is called and its `isDefinedAt()` is evaluated
twice (once in filter, once in apply's filter). This doubles the reflection cost on matching nodes
but also means multiple builders' results are combined.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/PlanUtils.java`, lines 73–96.

The report mentions the double `isDefinedAt()` from Group 1 but does not explicitly note that ALL
matching builders fire (not just the first), which could cause duplicate dataset registrations for
overlapping patterns.

### MISSING — `InputFieldsCollector.collect()` visits ALL plan nodes including Dataset-API nodes

Unlike `OutputFieldsCollector` which stops early, `InputFieldsCollector.collect()` is a full
recursive DFS that always descends into all children (lines 64–67). It calls
`discoverInputsFromNode()` for every node in the 6,778-node plan, including the 3,161 Dataset-API
nodes. Each call to `discoverInputsFromNode()` runs `extractDatasetIdentifier()` which has ~15
`instanceof` checks. For the 3,161 Dataset-API nodes, all 15 checks fail and the method returns
empty. This is consistent with F-10 but the specific code path (not `plan.foreach` but a custom
recursive DFS) means Java call stack depth of 134 levels. At typical JVM defaults (512+ frames),
this is not an overflow risk, but it is worth noting as a hidden allocation cost (stack frames).

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/InputFieldsCollector.java`, lines 52–68.

### MISSING — `InMemoryRelationInputDatasetBuilder` CAN trigger nested plan traversals

`InMemoryRelationInputDatasetBuilder.apply()` calls `plan.collect(delegate(...))` at line 47:

```java
plan.collect(delegate(context.getInputDatasetQueryPlanVisitors(),
                      context.getInputDatasetBuilders(), event))
```

This is `plan.collect()` on the **cached plan** associated with the InMemoryRelation, not the main
plan. If InMemoryRelation nodes are present (0 in this specific plan per the node type table), each
would trigger a full traversal of its cached sub-plan. The report notes InMemoryRelation as a
traversal driver in F-2 but does not analyze this nested-traversal path. With 0 InMemoryRelation
nodes in this plan it has no impact here, but it is a latent risk.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/InMemoryRelationInputDatasetBuilder.java`, lines 41–55.

### MISSING — `addDependency()` has a hard limit at 1,000,000 dependencies

`ColumnLevelLineageBuilder.addDependency()` returns early if `dependenciesAdded >
COMPUTED_DEPENDENCY_HARD_LIMIT` (= 1,000,000). Lines 111–114. With 259 Unions × ~96 columns each
= ~24,864 union-level dependency hops, plus Project/Filter/Aggregate expression dependencies across
2,804 nodes, it is plausible the hard limit could be hit for this plan. If so, `buildFields()`
at line 193 returns an empty `ColumnLineageDatasetFacetFields` with a warning log. The report does
not mention this guard.

Source: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java`, lines 46, 111–114, 193–199.

---

## Speculative Claims (not verifiable from source)

### SPECULATIVE — D ≈ 400 dependency chain depth estimate

The claim that Union nodes produce dependency chains of depth D ≈ 400 is structurally plausible:
each Union adds one hop per column position. With 259 Unions in a chain and ~96 columns each,
D could reach 259 Union hops per column plus additional hops from Project/Aggregate expression
trees.

However:
1. Whether the 259 Unions are arranged sequentially (maximum depth) or in a tree shape
   (log depth) cannot be determined from static analysis.
2. The actual ExprId graph topology depends on the query execution plan, which was not provided.
3. D ≈ 400 could be an overestimate if Unions are tree-shaped (D ≈ log₂(259) ≈ 8).

The O(D²) bottleneck is real regardless of D, but the magnitude estimate (126M comparisons)
could be off by 2–3 orders of magnitude if the Union tree is balanced rather than linear.

### SPECULATIVE — "~61M visitor object allocations/exec" (F-3)

The 1,258 Project × 96 expressions × 7 expr nodes × 18 visitors = 15.2M per CLL invocation
calculation uses three unverified constants: "96 avg expressions per Project," "7 expr nodes per
expression," and "18 visitor objects." All are estimates. The range could span from ~1M to ~200M
depending on actual expression tree structure.

### SPECULATIVE — Percent of 310 Relations that are Iceberg/Delta/Hive-managed

F-12 and F-14 assume 50 Iceberg relations or 100 Hive-managed relations respectively out of 310
total Relation nodes. These percentages are not derivable from the plan statistics alone. The
Dataset[T] pipeline may use entirely in-memory or file-based relations with no Iceberg or Hive
backing, in which case F-12 and F-14 have zero impact on this specific plan.

### SPECULATIVE — "CLL produces empty or broken lineage for this plan"

The claim that CLL produces entirely broken column lineage (I-1) because of Dataset-API encoder
boundaries is directionally correct — SerializeFromObject/DeserializeToObject do sever expression
chains. However, the 1,258 Project + 858 Filter + 205 Aggregate + 224 Join + 259 Union = 2,804
SQL-API nodes do propagate ExprId dependencies correctly through CLL. Whether any chain from the
InsertInto root's output ExprIds traces back through only SQL-API nodes to Relation inputs (without
crossing a Dataset-API node boundary) cannot be determined without runtime ExprId tracing. It is
possible that partial lineage is produced for columns whose lineage path avoids Dataset-API nodes.

---

## Summary Verdict

### Overall Reliability Rating: **Moderate — Core algorithmic findings are sound; calibrated numbers are unreliable**

**High-confidence findings (trust these):**
- F-1 (reflection in isDefinedAt): Real, confirmed in source. The per-call overhead is real even if the exact millisecond estimate is speculative.
- F-5 (O(D²) BFS in findDependentInputs): Confirmed exactly as described. LinkedList visited-set is the code at lines 284–306.
- F-7 (UnionVisitor LinkedList): Confirmed but correctly rated marginal.
- F-8 (IcebergMergeIntoVisitor allocation per node): Confirmed in source.
- F-10/F-11 (Dataset-API nodes not handled by CLL): Confirmed in source.
- The "CLL runs on every event for this plan" conclusion: Confirmed — schemaFacet reflects the target table schema (~96 fields), well under the 1,000 limit.

**Findings requiring revision before acting on them:**
- **F-0 / Part 7 SubqueryAlias "new finding"**: INCORRECT. The 174 × 6,778 × 15 = 17.7M extra node visits does not occur. `SubqueryAliasInputDatasetBuilder.delegate()` applies local visitors to a SINGLE child node, not the full plan. This "CRITICAL — single largest CPU cost" label is wrong and should be removed.
- **F-4 (addOutput O(S) schema scan)**: OVERSTATED. The 29,850 call count and 790-field scan assumption are both wrong. `OutputFieldsCollector` does not visit all 6,778 nodes, and `schema.getFields()` is the target table schema (~96 fields), not the count of unique ExprIds across the plan. The fix is still valid but the magnitude is not 94M comparisons per invocation.
- **F-2 traversal count**: Approximately correct (3–4 traversals) but the 4th (OutputFieldsCollector) is not a full-plan traversal. CLL invocation count per exec (stated as "~4") is unverified.
- **F-3 ExpressionTraverser allocations**: Direction correct, exact magnitude unverifiable.
- **F-12 CatalogUtils3 claim**: The "310 Relations × 4 calls" multiplier assumes all Relations are DataSourceV2Relation. Likely overstated for a Dataset[T] pipeline.

**Priority table from the report**: Rankings 1 (F-5) and 3 (F-1) and 2 (F-4) are the most defensible quick wins based on confirmed source code. Ranking 4 (F-3) is also defensible. The SubqueryAlias "finding" (unlisted in the table but described as potentially the largest cost) should be removed from the action list entirely — it is not supported by the code.
