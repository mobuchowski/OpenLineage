# Performance Audit - Group 8: Column-Level Lineage Operator Visitors

## Summary

The operator visitor layer itself is largely clean: JoinVisitor does NOT perform an O(n*m) cross-product over output attributes (it only traverses the join condition expression), and UnionVisitor's complexity is linear in columns multiplied by children. The most significant performance risks are concentrated in two places: (1) `IcebergMergeIntoVisitor.isDefinedAt()` performs a fresh `Arrays.asList()` allocation and `getCanonicalName()` call on every single logical-plan node during the full-plan `foreach` traversal, and (2) `ColumnLevelLineageBuilder.findDependentInputs()` uses a `LinkedList` as the visited-set guard, causing an O(n^2) membership check during the BFS graph walk that resolves column lineage at build time. Two visitors (`DistinctVisitor` and `CreateTableAsSelectVisitor`) call `collectFromOperator` on a child node that will also be visited by the outer `plan.foreach` traversal, causing duplicate dependency registration for those subtrees.

---

## Classes Audited

- `OperatorVisitor`: Interface only; defines `isDefinedAt` / `apply` contract.
- `JoinVisitor`: Handles Join operator; traverses only the condition expression, not output attribute pairs.
- `UnionVisitor`: Handles Union operator; transposes column positions across children — linear in columns x children.
- `AggregateVisitor`: Handles Aggregate operator; registers grouping and aggregate expressions; uses Set for ExprId deduplication in `doesGroupByAllAggregateExpressions`.
- `ProjectVisitor`: Handles Project operator; iterates project list and delegates to ExpressionTraverser.
- `FilterVisitor`: Handles Filter operator; creates a synthetic dataset-dependency ExprId and traverses the condition expression.
- `SortVisitor`: Handles Sort operator; creates a synthetic dataset-dependency ExprId and traverses sort order expressions.
- `DistinctVisitor`: Handles Distinct operator; delegates to `collectFromOperator` on the child node (double-traversal risk).
- `GenerateVisitor`: Handles Generate/EXPLODE operator; maps each generator output attribute to each generator child input expression — O(outputs * inputs) traversals.
- `WindowVisitor` (operator): Handles Window operator; iterates window expressions and delegates to ExpressionTraverser.
- `CreateTableAsSelectVisitor`: Handles CTAS operator; delegates to `collectFromOperator` on the query child (double-traversal risk).
- `DataSourceV2RelationVisitor`: Delegates to `QueryRelationColumnLineageCollector`; only fires when extension lineage is present.
- `IcebergMergeIntoVisitor`: Handles Iceberg MERGE INTO; uses reflection and allocates per-call; has O(n) class-name lookup in `isDefinedAt`.
- `ColumnLevelLineageBuilder` (supporting): Stores ExprId dependency graph; `findDependentInputs` uses a `LinkedList` as visited set.
- `ExpressionTraverser` (supporting): Walks expression trees; no depth limit but bounded by Spark's own expression tree depth.
- `ExpressionDependencyCollector` (supporting): Drives full-plan traversal via `plan.foreach`.
- `VisitorFactory`: Instantiates fresh visitor objects on every call to `operatorVisitors()` / `expressionVisitors()`.

---

## Performance Issues Found

### Issue 1: `IcebergMergeIntoVisitor.isDefinedAt` — Allocates a List and Calls `getCanonicalName` on Every Plan Node — Severity: MEDIUM

**Class**: `IcebergMergeIntoVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/operator/IcebergMergeIntoVisitor.java:40-43`

**Description**: `isDefinedAt` is called once per logical-plan node during the full `plan.foreach` traversal inside `ExpressionDependencyCollector.collectFromOperator`. For every node it (a) allocates a new `ArrayList` via `Arrays.asList(...)`, and (b) calls `operator.getClass().getCanonicalName()` — a relatively expensive string construction — then does a linear `contains` scan over a 2-element list.

**Root Cause**: `isDefinedAt` could use two direct `instanceof` checks or pre-cache the two `Class` objects and use `Class.isInstance`. Instead it constructs a temporary list and performs a string-based contains check on every invocation.

**Code Evidence**:
```java
// IcebergMergeIntoVisitor.java:40-43
@Override
public boolean isDefinedAt(LogicalPlan operator) {
  return Arrays.asList(MERGE_INTO_CLASS_NAME, MERGE_ROWS_CLASS_NAME)
      .contains(operator.getClass().getCanonicalName());
}
```

The `Arrays.asList()` call constructs a new fixed-size `List` wrapper on every call. `getCanonicalName()` constructs a `String` from internal JVM metadata every time it is called.

**Recommendation**: Cache the two `Class` objects as static fields at class load time (using `Class.forName` with a fallback to `null` if the Iceberg extension is not on the classpath), then use `Class.isInstance(operator)` in `isDefinedAt`. This reduces the check to two reference comparisons with zero allocation and zero string construction.

---

### Issue 2: `IcebergMergeIntoVisitor.apply` — Repeated `getMethods()` Array Allocation for Version Detection — Severity: MEDIUM

**Class**: `IcebergMergeIntoVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/operator/IcebergMergeIntoVisitor.java:51-62`

**Description**: Every time `apply` is called on a `MergeRows` node, it calls `mergeRows.getMethods()` which returns a freshly allocated `Method[]` array containing all public methods of the class. It then wraps this array in another `Arrays.asList()` and streams over it with `.filter()` to check for the presence of a `matchedOutputs` method. This version-detection probe runs on every MERGE INTO statement execution.

**Root Cause**: The Iceberg API version detection is performed imperatively at call time rather than being computed once and cached. `Class.getMethods()` is a moderately expensive reflection operation that triggers internal JVM security checks and allocates a defensive copy of the method array.

**Code Evidence**:
```java
// IcebergMergeIntoVisitor.java:51-62
Class mergeRows = Class.forName(MERGE_ROWS_CLASS_NAME);

boolean matchedOutputsMethodExists =
    Arrays.asList(mergeRows.getMethods()).stream()
        .map(m -> m.getName())
        .filter(m -> MATCHED_OUTPUTS.equals(m))
        .findAny()
        .isPresent();
```

Additionally, `Class.forName(MERGE_ROWS_CLASS_NAME)` is called inside `apply` on every invocation rather than being cached.

**Recommendation**: Perform the version detection once at class initialization time (e.g., in a `static` initializer block or lazily cached `static volatile boolean`). Cache the resolved `Class` objects and `Method` references as static fields. This reduces every `apply` invocation from an uncached reflection probe to a static field read.

---

### Issue 3: `findDependentInputs` BFS Uses `LinkedList` as Visited Set — O(n^2) Membership Test — Severity: HIGH

**Class**: `ColumnLevelLineageBuilder`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java:284-306`

**Description**: `findDependentInputs` implements a BFS over the expression-dependency graph to resolve all transitive source inputs for a given output `ExprId`. The visited-set is stored as a `LinkedList<Dependency>`, and the duplicate-filter check is `.filter(dependency -> !dependentInputs.contains(dependency))`. `LinkedList.contains` is O(n) per call (linear scan). Since this filter is applied to each candidate in `newDependentInputs` at every BFS iteration, and `dependentInputs` grows with each wave, the overall cost is O(n^2) in the total number of distinct dependencies.

For wide schemas on complex plans (many joins, long union chains, or deeply nested subqueries), `findDependentInputs` is called once per output column per `buildFields()` invocation. With W output columns and D total dependencies, the cost is O(W * D^2) in the worst case.

**Root Cause**: The data structure choice for `dependentInputs` is a `LinkedList` rather than a `HashSet`. `Dependency` does implement proper `equals`/`hashCode` (using `Objects.hash(exprId, transformationInfo)`), so upgrading to a `HashSet` is a drop-in correctness-preserving change.

**Code Evidence**:
```java
// ColumnLevelLineageBuilder.java:285-302
List<Dependency> dependentInputs = new LinkedList<>();
dependentInputs.add(new Dependency(outputExprId, TransformationInfo.identity()));
// ...
while (continueSearch) {
  newDependentInputs =
      newDependentInputs.stream()
          // ...
          .filter(dependency -> !dependentInputs.contains(dependency)) // O(n) per element
          .collect(Collectors.toSet());

  dependentInputs.addAll(newDependentInputs);
  continueSearch = !newDependentInputs.isEmpty();
}
```

The method is also called once per element in `datasetDependencies` from `datasetDependencyInputs()`, so every Filter, Sort, Aggregate, and Join operator in the plan adds an additional O(D^2) invocation.

**Recommendation**: Change `dependentInputs` from `LinkedList<Dependency>` to `HashSet<Dependency>`. The final result list can be collected by iterating the set. This changes the per-iteration membership test from O(n) to O(1), reducing overall BFS cost to O(W * D).

---

### Issue 4: `DistinctVisitor` and `CreateTableAsSelectVisitor` Double-Visit Child Subtrees — Severity: MEDIUM

**Classes**: `DistinctVisitor`, `CreateTableAsSelectVisitor`
**Locations**:
- `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/operator/DistinctVisitor.java:23`
- `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/operator/CreateTableAsSelectVisitor.java:24`

**Description**: Both visitors call `collectFromOperator(builder, child)` on a child `LogicalPlan` node. Meanwhile, the driving traversal in `ExpressionDependencyCollector.collect` uses `plan.foreach`, which is a depth-first pre-order traversal that visits every node in the plan tree exactly once by itself, including all children of `Distinct` and `CreateTableAsSelect`. As a result, the child node will be visited twice: once by the explicit `collectFromOperator` call made from the visitor, and once by the outer `plan.foreach` loop when it reaches the same child node in normal traversal order.

This causes all dependencies registered from the child subtree to be added to the builder a second time. For `DistinctVisitor`, the child of `Distinct` in Spark's plan is typically a full `Aggregate` subtree. For `CreateTableAsSelectVisitor`, the `isDefinedAt` guard requires `operator.children() == null || operator.children().isEmpty()`, which would typically be false for a CTAS with a real query, meaning this visitor fires only for leaf-like CTAS nodes in practice — but the condition's semantics are surprising and could be a latent bug.

**Root Cause**: `plan.foreach` (Spark's `TreeNode.foreach`) is a recursive pre-order traversal that visits the current node first, then descends into all children. There is no mechanism to skip child subtrees once they have been manually visited inside a visitor's `apply` method.

**Code Evidence**:
```java
// DistinctVisitor.java:23 — child will also be visited by outer foreach
public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
  collectFromOperator(builder, ((Distinct) operator).child());
}

// ExpressionDependencyCollector.java:22-28 — drives the outer traversal
plan.foreach(
    operator -> {
      CustomCollectorsUtils.collectExpressionDependencies(context, operator);
      collectFromOperator(context.getBuilder(), operator);  // visits Distinct child again
      return scala.runtime.BoxedUnit.UNIT;
    });
```

For `CreateTableAsSelectVisitor`, the `isDefinedAt` guard (`operator.children() == null || operator.children().isEmpty()`) is a safeguard against this, but it relies on an undocumented children-list invariant and is semantically fragile.

**Recommendation**: For `DistinctVisitor`, the visitor's `apply` should directly map the child's output attributes to the parent's output attributes (identity mapping) rather than re-running full dependency collection on the child. For `CreateTableAsSelectVisitor`, document clearly why the `children().isEmpty()` guard is required, or replace the approach with a direct output-attribute identity walk that does not recurse.

---

### Issue 5: `VisitorFactory` Allocates New Visitor Instances on Every Call — Severity: LOW

**Class**: `VisitorFactory`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/VisitorFactory.java:32-57`

**Description**: `VisitorFactory.operatorVisitors()` and `expressionVisitors()` construct and return a new `Arrays.asList(new XxxVisitor(), ...)` list every time they are called. `ExpressionDependencyCollector` holds a single static `VisitorFactory` instance, so `operatorVisitors()` is called once per plan node visit (inside `collectFromOperator`). With a plan of N nodes, this allocates N lists plus N sets of visitor objects.

**Root Cause**: The visitor lists are stateless and immutable; they are safe to share across calls. There is no reason to allocate them on every invocation.

**Code Evidence**:
```java
// VisitorFactory.java:32-47 — new instances created every call
List<OperatorVisitor> operatorVisitors() {
  return Collections.unmodifiableList(
      Arrays.asList(
          new ProjectVisitor(),
          new GenerateVisitor(),
          // ... 9 more new XxxVisitor()
          new IcebergMergeIntoVisitor()));
}
```

**Recommendation**: Make the two lists static final fields initialized once at class load time. All current visitor implementations are stateless (no instance fields mutated after construction), so a single shared instance of each list is correct and thread-safe.

---

### Issue 6: `ExpressionTraverser.isMasking` Linear Scan of Static Class Lists — Severity: LOW

**Class**: `ExpressionTraverser`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ExpressionTraverser.java:48-51`

**Description**: `isMasking` is called on every expression node that falls through to the generic fallback traversal. It performs two linear scans: one over `classes` (a `List<Class>` of 8 elements) comparing class objects, and one over `classNames` (a `List<String>` of 1 element) comparing canonical name strings. The lists are static fields, so they are allocated only once, but the scan itself is O(k) per expression node where k is the list size.

**Root Cause**: Both `classes` and `classNames` are used as sets but are stored as `List`. An `instanceof` chain or a `HashSet<Class>` lookup would be O(1).

**Code Evidence**:
```java
private static Boolean isMasking(Expression expression) {
  return classes.stream().anyMatch(c -> c.equals(expression.getClass()))
      || classNames.stream().anyMatch(n -> n.equals(expression.getClass().getCanonicalName()));
}
```

**Recommendation**: Replace `classes` with a `Set<Class>` and use `classes.contains(expression.getClass())` for O(1) lookup. Replace the `classNames` single-element list with a direct string equality check, or similarly use a `Set<String>`. Avoid calling `getCanonicalName()` repeatedly; if the string set is needed, cache the result of `expression.getClass().getCanonicalName()` in a local variable.

---

### Issue 7: `UnionVisitor` Uses `LinkedList` as Intermediate Collection Then Accesses by Index — Severity: LOW

**Class**: `UnionVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/operator/UnionVisitor.java:44-59`

**Description**: `childrenAttributes` is declared as `List<ArrayList<Attribute>>` but instantiated as a `LinkedList`. Later code accesses it via `childrenAttributes.get(childIndex)` inside an `IntStream.range` lambda, making `childIndex`-indexed access an O(n) traversal of the linked list rather than O(1).

**Root Cause**: `LinkedList.get(index)` is O(n). With C union children and K columns, the inner loop performs C*(C-1)/2 indexed lookups, each O(C), giving O(C^2 * K) for the full `childrenAttributes` traversal — though in practice C (number of union branches) is typically small (2–5).

**Code Evidence**:
```java
// UnionVisitor.java:44-45, 57-59
List<ArrayList<Attribute>> childrenAttributes = new LinkedList<>();
// ...
IntStream.range(1, children.size())
    .mapToObj(childIndex -> childrenAttributes.get(childIndex)  // O(childIndex) on LinkedList
```

**Recommendation**: Change `childrenAttributes` declaration to `ArrayList<ArrayList<Attribute>>` (or `List<List<Attribute>>` backed by `ArrayList`) to restore O(1) index access.

---

## Clean Classes

- **`JoinVisitor`**: Contrary to the initial concern, `JoinVisitor` does NOT attempt to enumerate or cross-product the output attributes of the left and right sides. It only traverses the join condition expression (if present) and registers it as a single dataset-level indirect dependency. The complexity is O(depth of condition expression tree), which is bounded and correct.

- **`AggregateVisitor`**: Uses `Set<ExprId>` for both `aggregateExprIds` and `groupingExprIds` in `doesGroupByAllAggregateExpressions`, giving O(1) per-element lookup. The `groupingExprIds.containsAll(aggregateExprIds)` call iterates over one set and probes the other — O(n) total. Clean.

- **`ProjectVisitor`**: Delegates directly to `ExpressionTraverser` per output expression. No structural complexity issues. Clean.

- **`FilterVisitor`**: Creates one synthetic `ExprId` and traverses one condition expression. Minimal and correct. Clean.

- **`SortVisitor`**: Creates one synthetic `ExprId` and traverses the sort order list. The complexity is O(number of sort keys * expression depth). Clean.

- **`WindowVisitor` (operator)**: Iterates `windowExpressions` and delegates to `ExpressionTraverser`. Linear in the number of window expressions. Clean.

- **`DataSourceV2RelationVisitor`**: Guards with a cheap `instanceof` check followed by a field-presence check. Only fires for extension nodes. Delegates to a separate collector. Clean.

- **`GenerateVisitor`**: The O(outputs * inputs) cross-product traversal is intentional — each generated output column depends on each generator input expression. In typical EXPLODE usage this is 1 output x 1-2 inputs. Acceptable for the use case, not a pathological concern.

---

## Spark/Iceberg Internals Investigated

**`~/perf-audit/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala`**

- `case class Join` (line 714): `output` is computed as `Join.computeOutput(joinType, left.output, right.output)`, which is a flat concatenation (`leftOutput ++ rightOutput`) with no cross-product. This confirms that `JoinVisitor`'s decision to only traverse the condition expression is correct — there are no output attribute pairs to iterate.

- `case class Union` / `abstract class UnionBase` (lines 501, 632): `output` is computed by `Union.mergeChildOutputs(children.map(_.output))`, which uses `childOutputs.transpose` (Scala's transpose) to group column positions across children, then takes the first child's attribute identity for each position. The transpose operation is O(children * columns) — the same complexity as `UnionVisitor`'s Java reimplementation. The `LAZY_SET_OPERATOR_OUTPUT` config flag controls whether output is lazily cached (`lazy val`) or recomputed on each access; OpenLineage's `UnionVisitor` does not rely on `union.output()` at all, bypassing this.

- `case class Aggregate` (line 1211): `output` returns `aggregateExpressions.map(_.toAttribute)`, a simple O(n) map. Confirms `AggregateVisitor`'s direct use of `aggregate.aggregateExpressions()` and `aggregate.groupingExpressions()` is correct.

- `TreeNode.foreach` (line 266–268): Pre-order depth-first traversal — visits `this` first, then recurses into `children`. No depth limit, no cycle detection. This confirms Issue 4: child nodes of `Distinct` and `CreateTableAsSelect` will be visited by both the explicit visitor `apply` call and the outer `plan.foreach`.

**`~/perf-audit/iceberg`**: Not directly read; Iceberg internals were examined via the reflection-based access patterns in `IcebergMergeIntoVisitor` and the class/method name constants defined therein (`MergeRows`, `MergeInto`, `matchedOutputs`, `notMatchedOutputs`).
