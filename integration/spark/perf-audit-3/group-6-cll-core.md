# Performance Audit - Group 6: Column-Level Lineage Core Engine

## Summary

The column-level lineage engine performs at minimum four separate O(N) traversals over the logical plan tree per query, where N is the number of plan nodes. The most significant bottleneck is the BFS cycle-detection in `findDependentInputs`, which uses `LinkedList.contains` (O(D) per check) inside a loop that grows proportionally to total dependencies, producing O(D^2) behavior per output field resolution. A secondary structural problem is that `ExpressionTraverser` allocates 18 fresh visitor objects per expression node traversal, generating substantial GC pressure on complex expression trees.

## Classes Audited

- `ColumnLevelLineageBuilder`: Central data store for inputs, outputs, and expression dependencies; implements BFS resolution of dependency chains at build time
- `ColumnLevelLineageUtils` (shared): Dispatcher that reflectively invokes the Spark3 implementation
- `ColumnLevelLineageUtils` (spark3): Orchestrates the full CLL pipeline: output collection, input/dependency collection, InMemoryRelation handling, facet construction
- `ColumnLevelLineageVisitor`: Interface only; no algorithmic content
- `ExpressionDependencyCollector`: Walks plan via `plan.foreach` to collect operator-level expression dependencies
- `ExpressionTraverser`: Recursively resolves expression trees, mapping child expressions to their output ExprId
- `InputFieldsCollector`: Manually recursively walks the plan tree to register leaf-node input fields
- `OutputFieldsCollector`: Manually recursively walks the plan tree to find and register output expressions
- `QueryRelationColumnLineageCollector`: Handles SQL-query-backed DataSourceV2 relations; parses SQL and maps column lineage via external ExprId mappings
- `CustomCollectorsUtils`: Delegates to context-registered `ColumnLevelLineageVisitor` instances per plan node
- `VisitorFactory`: Creates lists of operator and expression visitor instances

## Performance Issues Found

### [BFS Cycle Detection Uses O(D) LinkedList.contains Per Iteration] - Severity: HIGH
**Class**: `ColumnLevelLineageBuilder`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java` approx line 284
**Description**: The `findDependentInputs` method implements BFS over the `exprDependencies` graph using a `LinkedList<Dependency>` as the visited set. On every iteration, the deduplication filter calls `!dependentInputs.contains(dependency)`, which performs a linear O(D) scan over the entire already-visited list for each candidate dependency. As the BFS frontier expands over D total dependency nodes, the total work becomes O(D^2).
**Root Cause**: `LinkedList` does not provide O(1) membership testing. The choice to use a `List` instead of a `Set` as the visited-node store turns the visited-check into a linear scan executed once per candidate per BFS round.
**Code Evidence**:
```java
List<Dependency> dependentInputs = new LinkedList<>();   // visited set as a list
...
.filter(dependency -> !dependentInputs.contains(dependency)) // O(D) per check
```
**Recommendation**: Replace `dependentInputs` with a `HashSet<Dependency>`. `Dependency` already has correct `equals`/`hashCode` implementations (`Objects.hash(exprId, transformationInfo)`), so membership testing becomes O(1). Maintain a separate list only if the final ordered result is required, or return the Set directly.

---

### [Four Independent Full Plan-Tree Traversals Per CLL Invocation] - Severity: HIGH
**Class**: `ColumnLevelLineageUtils` (spark3), `ExpressionDependencyCollector`, `InputFieldsCollector`, `OutputFieldsCollector`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ColumnLevelLineageUtils.java` approx line 58
**Description**: For each query event, `buildColumnLineageDatasetFacet` triggers four separate complete traversals over the logical plan tree of size N:
1. `OutputFieldsCollector.collect` — manual recursive DFS (O(N))
2. `ExpressionDependencyCollector.collect` — `plan.foreach` (O(N))
3. `InputFieldsCollector.collect` — manual recursive DFS (O(N))
4. An additional `plan.foreach` scanning for `InMemoryRelation` nodes (O(N))

Each traversal independently visits every plan node. For queries involving cached datasets (`InMemoryRelation`), each cached plan also triggers recursive re-execution of steps 2 and 3.
**Root Cause**: Each collector was developed independently with no shared traversal pass. There is no single-pass visitor dispatch that combines output collection, dependency collection, and input collection in one walk.
**Code Evidence**:
```java
OutputFieldsCollector.collect(context, plan);           // pass 1
collectInputsAndExpressionDependencies(context, plan);  // passes 2, 3, and 4:
  ExpressionDependencyCollector.collect(context, plan); //   plan.foreach (pass 2)
  InputFieldsCollector.collect(context, plan);          //   manual DFS (pass 3)
  plan.foreach(node -> { /* InMemoryRelation scan */ }) //   plan.foreach (pass 4)
```
**Recommendation**: Unify into a single `plan.foreach` dispatch that routes each node simultaneously to output collection, dependency collection, and input collection. This reduces plan-tree traversals from 4 to 1 (or 2 if the output phase must precede dependency/input phases due to ordering constraints).

---

### [New VisitorFactory (18 Fresh Visitor Objects) Allocated Per Expression Node] - Severity: MEDIUM
**Class**: `ExpressionTraverser`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ExpressionTraverser.java` approx line 84
**Description**: Every call to `ExpressionTraverser.of(...)` constructs a `new VisitorFactory()`. `VisitorFactory.operatorVisitors()` and `expressionVisitors()` each construct a fresh `Arrays.asList(...)` containing 12 operator visitor instances and 6 expression visitor instances respectively, totaling 18 object allocations per `ExpressionTraverser` creation. Since `copyFor` calls `ExpressionTraverser.of`, every recursive step of expression traversal allocates 18 new visitor objects. For a complex expression tree with E nodes across F output columns this results in O(F * E) visitor allocations, all subject to GC.
**Root Cause**: The `VisitorFactory` is stateless but is created as an instance variable on each `ExpressionTraverser` rather than being shared. `ExpressionDependencyCollector` correctly uses a `static final VisitorFactory` but `ExpressionTraverser` does not follow this pattern.
**Code Evidence**:
```java
// ExpressionTraverser.of() - called for every sub-expression:
return new ExpressionTraverser(
    expression, outputExpressionId, transformationInfo, builder, new VisitorFactory()); // 18 new objects

// copyFor() recursively calls of():
public ExpressionTraverser copyFor(Expression expression) {
    return ExpressionTraverser.of(...); // triggers new VisitorFactory() again
}
```
```java
// VisitorFactory.operatorVisitors() allocates 12 new objects every call:
return Collections.unmodifiableList(Arrays.asList(
    new ProjectVisitor(), new GenerateVisitor(), new CreateTableAsSelectVisitor(),
    new DistinctVisitor(), new AggregateVisitor(), new JoinVisitor(), new FilterVisitor(),
    new SortVisitor(), new WindowVisitor(), new DataSourceV2RelationVisitor(),
    new UnionVisitor(), new IcebergMergeIntoVisitor()));
```
**Recommendation**: Make `VisitorFactory` a `static final` field of `ExpressionTraverser`, or pass the parent's `visitorFactory` reference through `copyFor` without re-constructing it. Since all visitors are stateless, a single shared instance per JVM is safe.

---

### [addOutput Scans Entire Schema List On Every Call] - Severity: MEDIUM
**Class**: `ColumnLevelLineageBuilder`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java` approx line 92
**Description**: `addOutput(ExprId, String)` performs a linear scan over `schema.getFields()` (size S) to find a matching field by name before registering the output. This is called once per expression in each plan node's `output()` list. With N plan nodes each exposing approximately W output columns, the total schema scan cost is O(N * W * S). Since W approaches S for wide schemas (intermediate nodes expose near the full schema), this degrades toward O(N * S^2). A second O(S) scan occurs in `getInputsUsedFor(String)`, called once per output field during `buildFields`, contributing an additional O(S^2) term.
**Root Cause**: Schema fields are stored in a `List` with no accompanying index structure for name-based lookup.
**Code Evidence**:
```java
public void addOutput(ExprId exprId, String attributeName) {
    schema.getFields().stream()
        .filter(field -> field.getName().equals(attributeName))  // O(S) scan each call
        .findAny()
        .ifPresent(field -> outputs.putIfAbsent(field, exprId));
}

List<TransformedInput> getInputsUsedFor(String outputName) {
    Optional<OpenLineage.SchemaDatasetFacetFields> outputField =
        schema.getFields().stream()
            .filter(field -> field.getName().equalsIgnoreCase(outputName))  // O(S) scan again
            .findAny();
    ...
}
```
**Recommendation**: Build a `Map<String, SchemaDatasetFacetFields>` index from `schema.getFields()` once in the constructor. Use it for O(1) lookups in both methods. Note that `addOutput` uses case-sensitive matching while `getInputsUsedFor` uses `equalsIgnoreCase` — this case-sensitivity inconsistency should also be resolved.

---

### [datasetDependencyInputs Runs Full BFS Per Entry Without Result Caching] - Severity: MEDIUM
**Class**: `ColumnLevelLineageBuilder`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java` approx line 327
**Description**: `datasetDependencyInputs()` iterates over every entry in `datasetDependencies` and calls `getInputsUsedFor(exprId)` for each, which internally runs `findDependentInputs` (the O(D^2) BFS). The `datasetDependencies` list accumulates one synthetic ExprId per `Join`, `Filter`, `Sort`, and `Aggregate` node encountered during plan traversal. For a plan with X such nodes, this triggers X independent BFS runs at O(D^2) each. Furthermore, `datasetDependencyInputs()` is called in both `buildFields` (when `datasetLineageEnabled` is false) and `buildDatasetDependencies`, meaning the entire computation may execute twice.
**Root Cause**: No memoization of BFS results. `findDependentInputs(ExprId)` recomputes from scratch on every invocation.
**Code Evidence**:
```java
private List<TransformedInput> datasetDependencyInputs() {
    return datasetDependencies.stream()               // X entries (joins + filters + sorts + aggregates)
        .flatMap(e -> getInputsUsedFor(e).stream())   // BFS O(D^2) per entry, no caching
        .distinct()
        .collect(Collectors.toList());
}
// Called in buildFields() when !datasetLineageEnabled AND in buildDatasetDependencies()
```
**Recommendation**: Memoize `findDependentInputs(ExprId)` results in a `HashMap<ExprId, List<Dependency>>`. Compute `datasetDependencyInputs()` at most once and store the result in a field if it may be needed from multiple call sites.

---

### [Redundant Reflection Calls in IcebergMergeIntoVisitor] - Severity: MEDIUM
**Class**: `IcebergMergeIntoVisitor`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/operator/IcebergMergeIntoVisitor.java` approx lines 51–101
**Description**: When handling `MergeRows` operators, the visitor performs `Class.forName(MERGE_ROWS_CLASS_NAME)` on every invocation (uncached). It then calls `mergeRows.getMethods()` — which returns a full freshly-allocated copy of all declared and inherited methods — to check for the existence of `matchedOutputs`. The `matchedOutputs` method is then obtained via `getMethod()` and invoked three separate times: once for the `isNewerImplementation` probe and once more to fetch the actual data (the third invocation is implicit via the path split). In the newer-implementation path where `matched.size() > 1`, `fromSeqNestedThreeTimes(matched)` and `fromSeqNestedTwice(notMatched)` are each called twice on the same already-retrieved `Seq` objects, performing redundant full Scala-to-Java collection conversions.
**Root Cause**: No caching of the class reference, method handles, or the version-detection result. Collection conversion results not stored in local variables.
**Code Evidence**:
```java
Class mergeRows = Class.forName(MERGE_ROWS_CLASS_NAME);           // uncached; called per operator
Arrays.asList(mergeRows.getMethods()).stream()...                  // getMethods() allocates full array
...
mergeRows.getMethod(MATCHED_OUTPUTS).invoke(operator)             // 1st invoke for isNewerImplementation
...
mergeRows.getMethod(MATCHED_OUTPUTS).invoke(operator)             // 2nd invoke for actual data (redundant)
...
fromSeqNestedThreeTimes(matched).get(0)  // full Scala->Java conversion #1
fromSeqNestedThreeTimes(matched).get(1)  // full Scala->Java conversion #2 (redundant, same input)
fromSeqNestedTwice(notMatched)           // full Scala->Java conversion #3
fromSeqNestedTwice(notMatched)           // full Scala->Java conversion #4 (redundant, same input)
```
**Recommendation**: Cache the `Class` objects and `Method` references as `private static final` fields initialized once. Store the results of `fromSeqNestedThreeTimes` and `fromSeqNestedTwice` in local variables before use in multiple `collect` calls.

---

### [isAssignableFrom Direction Inverted in InputFieldsCollector — Dead Code Branch] - Severity: LOW
**Class**: `InputFieldsCollector`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/InputFieldsCollector.java` approx line 59
**Description**: The condition `(plan.getClass()).isAssignableFrom(UnaryNode.class)` checks whether `UnaryNode` is a subtype of `plan.getClass()`. Since `plan` is always a concrete class (e.g., `Filter`, `Project`), this condition is always `false` for any concrete plan node. The `UnaryNode` optimized path (which would call `.child()` directly) is therefore never taken. All `UnaryNode` subtypes instead fall through to the `else if (plan.children() != null)` branch, which iterates `children()` (returning a list with one element). This is functionally equivalent but bypasses the stated intent of the workaround.
**Root Cause**: The `isAssignableFrom` arguments are transposed. The comment states this is a workaround for a Spark 3.2.1 `IncompatibleClassChangeError` with `instanceof UnaryNode`, but the workaround does not achieve its goal.
**Code Evidence**:
```java
// WRONG: condition is always false for concrete plan nodes
if ((plan.getClass()).isAssignableFrom(UnaryNode.class)) {
    collect(context, ((UnaryNode) plan).child());
}
// Correct form: UnaryNode.class.isAssignableFrom(plan.getClass())
```
**Recommendation**: Correct the `isAssignableFrom` argument order, or use a string class-name check as an alternative workaround for the Spark 3.2.1 bug. The current code is dead code and gives no performance benefit.

---

### [ExpressionTraverser.isMasking Scans Lists via Stream Per Expression Node] - Severity: LOW
**Class**: `ExpressionTraverser`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ExpressionTraverser.java` approx line 48
**Description**: `isMasking(Expression)` is called in the generic fallback branch of `traverse()` for every expression child node that does not match a specific visitor. It allocates a `Stream` to linearly scan `classes` (8 `Class` objects) and a second `Stream` to scan `classNames` (1 `String`). Since the vast majority of expressions are not masking functions, both streams are created and traversed to their end for no benefit on almost every call.
**Root Cause**: The masking class set is stored as `List<Class>` and accessed via `stream().anyMatch()` rather than a `Set.contains()` call.
**Code Evidence**:
```java
private static final List<Class> classes = Arrays.asList(Crc32.class, HiveHash.class, ...); // List, not Set

private static Boolean isMasking(Expression expression) {
    return classes.stream().anyMatch(c -> c.equals(expression.getClass()))  // Stream alloc + linear scan
        || classNames.stream().anyMatch(n -> n.equals(expression.getClass().getCanonicalName()));
}
```
**Recommendation**: Replace `List<Class>` with `private static final Set<Class<?>> MASKING_CLASSES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(...)))` and use `MASKING_CLASSES.contains(expression.getClass())`. This eliminates Stream allocation and provides O(1) lookup.

## Clean Classes

- **`ColumnLevelLineageVisitor`**: Pure interface, no algorithmic content.
- **`QueryRelationColumnLineageCollector`**: Logic is bounded by the number of column lineage entries in parsed SQL metadata, not by plan tree size. The `getDescendantId` scan is over the output attribute list, bounded by schema width.
- **`CustomCollectorsUtils`**: Simple delegation to the pre-built visitor list from context; no hidden iteration overhead beyond the visitor count.
- **`Dependency`**: Correct `equals`/`hashCode` via `Objects.hash(exprId, transformationInfo)`, enabling O(1) `HashSet` operations. The `merge` method is O(1).
- **`Input` / `TransformedInput`**: Correct `equals`/`hashCode`. No algorithmic concerns.
- **`ExpressionDependencyCollector`**: Correctly uses a `static final VisitorFactory` — avoids per-node allocation for operator visitors. Single `plan.foreach` is the right traversal pattern for this pass.
- **`AggregateVisitor`**: The `doesGroupByAllAggregateExpressions` helper creates two `HashSet`s via `Collectors.toSet()` and is called from `isDefinedAt`. It only runs for `Aggregate` nodes (not all plan nodes), and the sets are bounded by the number of aggregate expressions in that single node — acceptable cost.
- **Operator and expression visitors** (ProjectVisitor, JoinVisitor, FilterVisitor, SortVisitor, operator WindowVisitor, AliasVisitor, CaseWhenVisitor, IfVisitor, CoalesceVisitor, AggregateExpressionVisitor, expression WindowVisitor, UnionVisitor): All are stateless, bounded in cost to the expressions within the matched operator node, and contain no nested plan-level traversal.
- **Hard limits in `ColumnLevelLineageBuilder`**: `COMPUTED_DEPENDENCY_HARD_LIMIT` (1,000,000) and `RETURNED_INPUT_FIELD_LIMIT` (100,000) prevent runaway execution. The `schemaSizeLimit` check (default 1,000 fields) in `ColumnLevelLineageUtils` provides an early-exit for extremely wide schemas, protecting all subsequent computation.

## Spark/Iceberg Internals Investigated

**`spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala`**
- `foreach(f: BaseType => Unit)`: Pre-order depth-first traversal, calls `f(this)` then `children.foreach(_.foreach(f))`. Cost is strictly O(N) where N is the total number of nodes in the subtree. No memoization and no cycle detection — a DAG with shared subtrees would visit shared nodes once per reference path.
- `collect(pf: PartialFunction)`: Implemented on top of `foreach`, also O(N). Allocates a mutable `ArrayBuffer`. Confirms that each call to `plan.foreach` or `plan.collect` in the OpenLineage code is a full O(N) walk.

**`spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/namedExpressions.scala`**
- `ExprId(id: Long, jvmId: UUID)`: `hashCode` is `id.hashCode()` — a single `Long.hashCode()` call, O(1). `equals` compares both `id` and `jvmId`, also O(1). `ExprId` is a safe and efficient `HashMap`/`HashSet` key. This confirms that `HashSet<Dependency>` (which hashes on `exprId` via `Dependency.hashCode`) would give O(1) `contains` checks and would eliminate the O(D^2) cost in `findDependentInputs`.
- `NamedExpression.newExprId()`: `AtomicLong.getAndIncrement()` — thread-safe and O(1). The synthetic ExprIds created by `JoinVisitor`, `FilterVisitor`, `SortVisitor`, and `AggregateVisitor` each incur this cost once per matched operator; this is negligible.
- `basicLogicalOperators.scala`: Confirmed the key plan node types (`Project`, `Filter`, `Join`, `Sort`, `Aggregate`, `Union`) and their `children` overrides. All delegate to `TreeNode.foreach` for tree traversal, confirming the O(N) cost per pass.
