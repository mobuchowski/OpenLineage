# Performance Audit - Group 11: Merge Into Dataset Builders

## Summary

The Merge Into visitor group has three distinct performance problems of varying severity. The most serious is a structural double-and-triple traversal of both source and target plan subtrees caused by the interaction between `MergeIntoDeltaColumnLineageVisitor.collectInputs` / `collectExpressionDependencies` explicitly re-invoking `InputFieldsCollector.collect` and `collectInputsAndExpressionDependencies` on subtrees that the outer tree-walking framework has already queued for visitation. Compounding this, `getOutputExprIdByFieldName` is a linear scan called O(N) times per merge action, and `MergeRowsColumnLineageVisitor` re-materializes the same Scala-to-Java list conversion O(columns * instructions) times per execution. The Iceberg and Databricks edge-command variants inherit or duplicate these patterns across all version-specific subclasses.

## Classes Audited

- `MergeIntoCommandInputDatasetBuilder`: Input dataset builder for Delta `MergeIntoCommand`; delegates to target + source via the standard visitor chain.
- `MergeIntoCommandOutputDatasetBuilder`: Output dataset builder for Delta `MergeIntoCommand`; iterates all registered output builders linearly in `jobNameSuffix`.
- `MergeIntoCommandEdgeInputDatasetBuilder`: Databricks runtime variant using reflection to extract target + source — two uncached `MethodUtils.invokeExactMethod` calls and a string-based class check per invocation.
- `MergeIntoCommandEdgeOutputDatasetBuilder`: Databricks output variant; same string-based class check and one uncached `invokeExactMethod` call.
- `MergeIntoDeltaColumnLineageVisitor`: Abstract base for Delta column lineage; triggers duplicate plan traversals in both `collectInputs` and `collectExpressionDependencies`, and performs repeated linear scans through the output schema map.
- `MergeIntoDelta11ColumnLineageVisitor`: Spark 3.2 Delta subclass; inherits all parent issues, uses `DeltaMergeIntoInsertClause`.
- `MergeIntoIceberg013ColumnLineageVisitor`: Spark 3.2 Iceberg variant; has a linear `while` descent to find `MergeRows` and calls `InputFieldsCollector.collect` on both child and table twice per node type.
- `MergeIntoDelta24ColumnLineageVisitor`: Spark 3.4 Delta subclass; inherits all parent issues, uses `DeltaMergeIntoNotMatchedClause`.
- `MergeIntoIceberg13ColumnLineageVisitor`: Spark 3.4 Iceberg variant; duplicates the Iceberg013 logic with `ReplaceIcebergData` / `WriteIcebergDelta`.
- `MergeRowsColumnLineageVisitor`: Spark 3.5 visitor for native `MergeRows` nodes; has an O(columns * instructions) repeated Scala-to-Java list conversion.
- `MergeIntoCommandEdgeColumnLineageBuilder`: Databricks Spark 3.4 column lineage builder; uses uncached reflection for every field access and repeats `getCanonicalName().endsWith()` three times per node visit.

---

## Performance Issues Found

### Duplicate Subtree Traversal in InputFieldsCollector — Severity: HIGH

**Class**: `MergeIntoDeltaColumnLineageVisitor`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/MergeIntoDeltaColumnLineageVisitor.java:31-55`

**Description**: `collectInputs` explicitly calls `InputFieldsCollector.collect(context, target)` and `InputFieldsCollector.collect(context, source)` when it encounters a `MergeIntoCommand` node. However, `InputFieldsCollector.collect` is already a recursive traversal that descends into `plan.children()` after calling `CustomCollectorsUtils.collectInputs`. Because `MergeIntoCommand`'s children include both `target` and `source`, the outer recursion will visit them again immediately after `collectInputs` returns. The result is that the target subtree is traversed at least twice by `InputFieldsCollector`, and the source subtree is traversed at least twice (and up to three times when `collectExpressionDependencies` calls `collectInputsAndExpressionDependencies(source)` — see the next issue).

**Root Cause**: The merge visitor incorrectly assumes it is operating as a standalone collector rather than being called inside a pre-existing recursive tree walk. It re-invokes subtree-wide collection on top of the outer recursion already in flight.

**Code Evidence**:
```java
// collectInputs — explicit subtree calls
InputFieldsCollector.collect(context, ((MergeIntoCommand) node).target()); // explicit traversal
...
InputFieldsCollector.collect(context, ((MergeIntoCommand) node).source()); // explicit traversal

// InputFieldsCollector.collect — outer recursion ALSO descends into the same children
} else if (plan.children() != null) {
    ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children()).stream()
        .forEach(child -> collect(context, child)); // visits target + source again
}
```

**Recommendation**: Remove the explicit `InputFieldsCollector.collect(target)` and `InputFieldsCollector.collect(source)` calls from `collectInputs`. Allow the outer recursion in `InputFieldsCollector.collect` to handle child traversal naturally. The merge-specific logic (filtering inputs by merge actions) can still be applied as a post-processing step after the outer traversal completes, or the filtering can be structured to not require re-triggering traversal.

---

### Third Source-Subtree Traversal via collectExpressionDependencies — Severity: HIGH

**Class**: `MergeIntoDeltaColumnLineageVisitor`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/MergeIntoDeltaColumnLineageVisitor.java:72-73`

**Description**: `collectExpressionDependencies` calls `ColumnLevelLineageUtils.collectInputsAndExpressionDependencies(context, source)` when processing a `MergeIntoCommand` node. This method internally calls both `ExpressionDependencyCollector.collect(source)` (which does `source.foreach(...)`) and `InputFieldsCollector.collect(source)`. However, `ExpressionDependencyCollector.collect` is already invoked on the entire `MergeIntoCommand` plan tree by the outer call in `ColumnLevelLineageUtils.buildColumnLineageDatasetFacet`, which visits every node in both `source` and `target` via `plan.foreach`. The source subtree therefore receives a full second pass of expression dependency collection. Spark's `TreeNode.foreach` is a pre-order DFS that visits all descendants: this was confirmed in `TreeNode.scala:266-268`.

**Root Cause**: The visitor assumes it is the sole entry point for collection, not one callback in an already-running tree walk driven by `ExpressionDependencyCollector.collect(MergeIntoCommand)`.

**Code Evidence**:
```java
// collectExpressionDependencies — triggers full source subtree walk
ColumnLevelLineageUtils.collectInputsAndExpressionDependencies(
    context, ((MergeIntoCommand) node).source()); // source.foreach() runs again

// Meanwhile, ExpressionDependencyCollector.collect already walked source nodes:
// plan.foreach(operator -> CustomCollectorsUtils.collectExpressionDependencies(context, operator))
// since plan = MergeIntoCommand and foreach visits all descendants including source subtree
```

**Recommendation**: Remove `collectInputsAndExpressionDependencies(source)` from `collectExpressionDependencies`. The expression dependency collection for source nodes is already performed by the outer `plan.foreach` in `ExpressionDependencyCollector`. Only the merge-action-to-output mapping (the `getMergeActions` stream and `addDependency` calls) needs to remain in `collectExpressionDependencies`.

---

### O(N * M) Linear Scan in getOutputExprIdByFieldName — Severity: MEDIUM

**Class**: `MergeIntoDeltaColumnLineageVisitor`, `MergeIntoCommandEdgeColumnLineageBuilder`
**Location**:
- `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/MergeIntoDeltaColumnLineageVisitor.java:43,83,92`
- `integration/spark/spark34/src/main/java/io/openlineage/spark34/agent/lifecycle/plan/column/MergeIntoCommandEdgeColumnLineageBuilder.java:67,103,112`

**Description**: `getOutputExprIdByFieldName(String field)` in `ColumnLevelLineageBuilder` performs a linear scan over `outputs.keySet()`:

```java
return outputs.keySet().stream()
    .filter(fields -> fields.getName().equals(field))
    .findAny()
    .map(f -> outputs.get(f));
```

This is O(M) per call (M = output schema field count). In `collectInputs`, it is called once per merge action in the filter predicate. In `collectExpressionDependencies`, it is called twice per action (once in the filter and once inside `forEach` to retrieve the ExprId for `addDependency`). For N merge actions and M output columns, total cost is O(N * M). Both `MergeIntoDeltaColumnLineageVisitor` and `MergeIntoCommandEdgeColumnLineageBuilder` independently exhibit this pattern.

**Root Cause**: The `outputs` map is `Map<SchemaDatasetFacetFields, ExprId>` (a `HashMap`) but is looked up by field-name string, bypassing the hash structure entirely. There is no secondary name-to-ExprId index.

**Code Evidence**:
```java
// In collectExpressionDependencies — two O(M) scans per action:
.filter(
    action ->
        context.getBuilder()
            .getOutputExprIdByFieldName(action.targetColNameParts().mkString())
            .isPresent())  // O(M) scan
.forEach(
    action ->
        context.getBuilder().addDependency(
            context.getBuilder()
                .getOutputExprIdByFieldName(action.targetColNameParts().mkString())  // O(M) again
                .get(), ...));
```

**Recommendation**: Add a `Map<String, ExprId> outputsByName` field to `ColumnLevelLineageBuilder`, populated on each `addOutput` call. Replace `getOutputExprIdByFieldName` with an O(1) `outputsByName.get(field)` lookup. Alternatively, use `flatMap` to call `getOutputExprIdByFieldName` exactly once per action and carry the result through, eliminating the duplicate scan within a single stream pipeline.

---

### O(N * M) List.contains() in Input Pruning — Severity: MEDIUM

**Class**: `MergeIntoDeltaColumnLineageVisitor`, `MergeIntoCommandEdgeColumnLineageBuilder`
**Location**:
- `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/MergeIntoDeltaColumnLineageVisitor.java:48-53`
- `integration/spark/spark34/src/main/java/io/openlineage/spark34/agent/lifecycle/plan/column/MergeIntoCommandEdgeColumnLineageBuilder.java:72-77`

**Description**: After building `mergeActionsExprIds` (a `List<ExprId>`), the code filters the builder's inputs keyset using `List.contains()`:

```java
List<ExprId> inputsToRemove =
    context.getBuilder().getInputs().keySet().stream()
        .filter(id -> !mergeActionsExprIds.contains(id))  // O(N) per check
        .collect(Collectors.toList());
```

`ArrayList.contains()` is O(N). With M entries in the inputs keyset and N items in `mergeActionsExprIds`, this is O(M * N) total. `ExprId` is a Spark case class with well-defined `equals`/`hashCode`, so a `HashSet` lookup would be O(1) average.

**Root Cause**: `mergeActionsExprIds` is collected with `Collectors.toList()` instead of `Collectors.toSet()`.

**Code Evidence**:
```java
List<ExprId> mergeActionsExprIds = getMergeActions(...)
    ...
    .collect(Collectors.toList());  // should be toSet()

List<ExprId> inputsToRemove = context.getBuilder().getInputs().keySet().stream()
    .filter(id -> !mergeActionsExprIds.contains(id))  // O(N) list scan per id
    .collect(Collectors.toList());
```

**Recommendation**: Change `Collectors.toList()` to `Collectors.toSet()` for `mergeActionsExprIds`. The `!mergeActionsExprIds.contains(id)` check then becomes O(1) average, reducing the filter to O(M).

---

### Repeated Scala-to-Java List Materialization in MergeRowsColumnLineageVisitor — Severity: MEDIUM

**Class**: `MergeRowsColumnLineageVisitor`
**Location**: `integration/spark/spark35/src/main/java/io/openlineage/spark35/agent/lifecycle/plan/MergeRowsColumnLineageVisitor.java:58-76`

**Description**: The outer loop iterates C output column positions; the inner loop iterates I instructions. Inside the inner body, `ScalaConversionUtils.fromSeq(instruction.outputs())` materializes a new Java `List` by copying the same Scala `Seq` on each (column, instruction) pair. Since `instruction.outputs()` is the same `Seq` for a given instruction regardless of which column position is being processed, this produces C redundant copies of the same list per instruction, for O(C * I) total `fromSeq` calls. `ScalaConversionUtils.fromSeq` calls `seq.toBuffer()` which is an O(size) copy.

**Root Cause**: The per-instruction list materialization is nested inside the outer column-position loop instead of being hoisted above it.

**Code Evidence**:
```java
IntStream.range(0, mergeRows.output().size())   // C iterations
    .forEach(position -> {
        instructions.stream()
            .forEach(instruction -> {
                ScalaConversionUtils.fromSeq(instruction.outputs()).stream()  // re-materialized C times per instruction
                    .map(s -> ScalaConversionUtils.<Expression>fromSeq((Seq<Expression>) s))
                    .filter(l -> l.size() > position)
                    .map(l -> l.get(position))
                    ...
            });
    });
```

**Recommendation**: Restructure so instructions are the outer loop and column positions are inner. Pre-materialize `instruction.outputs()` once per instruction before the inner loop:

```java
instructions.forEach(instruction -> {
    List<List<Expression>> rows = ScalaConversionUtils
        .<Seq<Expression>>fromSeq(instruction.outputs()).stream()
        .map(s -> ScalaConversionUtils.<Expression>fromSeq(s))
        .collect(Collectors.toList()); // materialized once per instruction

    IntStream.range(0, mergeRows.output().size()).forEach(position ->
        rows.stream()
            .filter(row -> row.size() > position)
            .map(row -> row.get(position))
            .filter(expr -> expr instanceof NamedExpression)
            .forEach(expr -> ExpressionTraverser.of(
                expr, mergeRows.output().apply(position).exprId(), context.getBuilder()).traverse())
    );
});
```

---

### Uncached Reflection in MergeIntoCommandEdge Builders — Severity: MEDIUM

**Class**: `MergeIntoCommandEdgeInputDatasetBuilder`, `MergeIntoCommandEdgeOutputDatasetBuilder`, `MergeIntoCommandEdgeColumnLineageBuilder`
**Location**:
- `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/MergeIntoCommandEdgeInputDatasetBuilder.java:41-42`
- `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/MergeIntoCommandEdgeOutputDatasetBuilder.java:39`
- `integration/spark/spark34/src/main/java/io/openlineage/spark34/agent/lifecycle/plan/column/MergeIntoCommandEdgeColumnLineageBuilder.java:140`

**Description**: All three Databricks edge-command handlers access fields (`target`, `source`, `matchedClauses`, `notMatchedClauses`) via `MethodUtils.invokeExactMethod` / `MethodUtils.invokeMethod` on every invocation without caching resolved `Method` objects. More significantly, the class-name guard (`node.getClass().getCanonicalName().endsWith(CLASS)`) in `MergeIntoCommandEdgeColumnLineageBuilder` is evaluated for every node in the entire plan tree (called from `CustomCollectorsUtils.collectExpressionDependencies` which is driven by `plan.foreach`). `getCanonicalName()` allocates a `String` on each call for non-array classes in many JVM implementations.

**Root Cause**: No caching of the resolved `Class` reference or `Method` objects; string-based class identity check repeated per plan-tree node.

**Code Evidence**:
```java
// Called on every node in the plan tree via plan.foreach
if (!node.getClass().getCanonicalName().endsWith(CLASS)) return;  // string alloc per node

// For matching nodes, uncached method lookups:
private <T> Optional<T> getFieldFromNode(LogicalPlan node, String field) {
    return Optional.of((T) MethodUtils.invokeMethod(node, field));  // method lookup each call
}
```

**Recommendation**: Cache the `Class<?>` reference for `MergeIntoCommandEdge` at construction time using `ReflectionUtils.lookupClass(...)`. Replace `getCanonicalName().endsWith(CLASS)` with `cachedClass.isInstance(node)`. Cache the four `Method` objects (`target`, `source`, `matchedClauses`, `notMatchedClauses`) as `static final` fields resolved once on first use via `MethodUtils.getMatchingAccessibleMethod`, then invoke them directly.

---

### jobNameSuffix Linear Builder Scan — Severity: LOW

**Class**: `MergeIntoCommandOutputDatasetBuilder`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/MergeIntoCommandOutputDatasetBuilder.java:51-57`

**Description**: `jobNameSuffix` iterates over all registered output dataset builders at runtime to find the first one that can produce a job-name suffix for the merge target plan. Each `jobNameSuffixFromLogicalPlan` call itself calls `isDefinedAtLogicalPlan` on the target, so the effective cost is O(B) builder checks per merge event (B = total registered output builders). This is bounded by the number of registered builders and called infrequently (once per job event rather than per plan node), so impact is low.

**Root Cause**: No direct reference to the responsible builder is retained; the code searches the full builder list at call time.

**Code Evidence**:
```java
return context.getOutputDatasetBuilders().stream()
    .filter(b -> b instanceof AbstractQueryPlanOutputDatasetBuilder)
    .map(b -> (AbstractQueryPlanOutputDatasetBuilder) b)
    .map(b -> b.jobNameSuffixFromLogicalPlan(target))  // isDefinedAt check per builder
    .filter(Optional::isPresent)
    .map(o -> (String) o.get())
    .findFirst();
```

**Recommendation**: Either cache the result on first computation per `MergeIntoCommand` identity, or inject a direct reference to the target-handling builder at construction time. Impact is low enough that this can be deferred.

---

## Clean Classes

**MergeIntoDelta11ColumnLineageVisitor** and **MergeIntoDelta24ColumnLineageVisitor**: These subclasses are clean in themselves. Their `getMergeActions` implementations perform a single two-way `Stream.concat` over the matched and not-matched clause lists, each converted from Scala sequences exactly once. The only inefficiency they carry is inherited from `MergeIntoDeltaColumnLineageVisitor`.

**MergeIntoCommandInputDatasetBuilder**: The `apply` method delegates to `delegate(target)` and `delegate(source)` with no additional logic. Both calls are structurally necessary since both trees represent inputs to a merge operation. No redundancy in this class.

**MergeIntoCommandEdgeInputDatasetBuilder** / **MergeIntoCommandEdgeOutputDatasetBuilder** (dataset level): Architecturally clean. They do the minimum needed: extract target and/or source via reflection and delegate. The reflection overhead exists but is tolerable at the dataset-extraction level since these run once per event.

**`ReplaceData`/`ReplaceIcebergData` path in Iceberg visitors**: The `collectExpressionDependencies` code for `ReplaceData` and `ReplaceIcebergData` (the `Project`-based indexed for-loop) is clean. `fromSeq` is called once for both `queryOutputs` and `outputs`, and subsequent indexed access uses Java `ArrayList.get(i)` which is O(1). The size mismatch guard prevents out-of-bounds access. No quadratic behavior.

**`WriteDelta`/`WriteIcebergDelta` while-loop descent**: The `while` loop that walks down first-child nodes to find `MergeRows` is O(depth), not O(tree size), and terminates immediately on finding `MergeRows`. The `fromSeq` calls inside the loop body execute only once (after the break condition is satisfied). This is acceptable.

---

## Spark/Iceberg Internals Investigated

- **`spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala` (lines 263-268)**: Confirmed `foreach` does pre-order DFS of all children. `MergeIntoCommand.foreach` therefore visits every node in both source and target subtrees, which is the structural cause of the duplicate-traversal issues: merge visitors' explicit sub-calls to `InputFieldsCollector.collect(target/source)` re-enter nodes that the outer `foreach` has already scheduled for visitation.

- **`spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/QueryPlan.scala` (line 57)**: `output` is declared abstract (`def output: Seq[Attribute]`). Concrete implementations use `val output` (eager, stored), so repeated calls do not recompute the attribute list. This means indexed access in the Iceberg visitors' for-loops does not compound with output-recomputation cost.

- **`spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceV2Relation.scala` (line 104, 206)**: `output` is `override val`, confirming O(1) repeated access.

- **`OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java`**: The `outputs` map is `HashMap<SchemaDatasetFacetFields, ExprId>`. `getOutputExprIdByFieldName` bypasses the hash structure and does a keyset stream-scan. A secondary `Map<String, ExprId>` populated in `addOutput` would give O(1) lookups. The `inputs` map uses `ExprId` as the key; Spark's `ExprId` is a case class with proper `hashCode`/`equals`, confirming that converting `mergeActionsExprIds` to a `HashSet` would give O(1) membership checks.

- **`OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ExpressionDependencyCollector.java`**: Uses `plan.foreach` to drive expression dependency collection across the whole tree. Confirmed this visits all descendants, meaning the explicit `collectInputsAndExpressionDependencies(source)` call in the merge visitor constitutes a redundant second pass of the source subtree.

- **`OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/InputFieldsCollector.java`**: After processing each node, unconditionally recurses into all children (lines 64-66). The interaction with merge visitors' explicit sub-calls produces the confirmed double and triple traversals.
