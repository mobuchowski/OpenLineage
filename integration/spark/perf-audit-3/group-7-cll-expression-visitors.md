# Performance Audit - Group 7: Column-Level Lineage Expression Visitors

## Summary

The expression visitor layer is structurally sound — all individual visitors (AliasVisitor, CaseWhenVisitor, CoalesceVisitor, IfVisitor, WindowVisitor) perform O(N) work proportional to the number of children at each node, with no algorithmic explosions in the visitor logic itself. However, three significant performance issues exist: (1) a `new VisitorFactory()` instance (and its 6 visitor objects) is allocated on every single recursive `copyFor()` call in `ExpressionTraverser`, causing enormous object churn in deeply nested expression trees; (2) `CoalesceVisitor` double-traverses every child sub-tree — once as indirect and once as direct — unnecessarily doubling work and dependency registrations; and (3) `AggregateExpressionVisitor` performs an uncached reflective method lookup via `MethodUtils.getAccessibleMethod()` on every aggregate expression encountered, which is expensive and entirely avoidable.

## Classes Audited

- `ExpressionVisitor`: Interface defining `isDefinedAt(Expression)` / `apply(Expression, ExpressionTraverser)` — the visitor contract.
- `ExpressionTraverser`: Core recursive traversal engine; creates `new VisitorFactory()` on every `copyFor()` call.
- `AliasVisitor`: Delegates to child expression; clean pass-through, O(1).
- `AggregateExpressionVisitor`: Registers resultId dependency and recurses into aggregateFunction; contains per-call reflective method lookup.
- `CaseWhenVisitor`: Traverses all WHEN conditions (indirect) and all THEN/ELSE values (direct); O(N) in branch count.
- `CoalesceVisitor`: Traverses every child twice (once indirect, once direct); O(2N).
- `IfVisitor`: Traverses predicate (indirect) + trueValue + falseValue; O(1) per node.
- `WindowVisitor`: Traverses windowFunction (unless RankLike/RowNumberLike) and all windowSpec children; O(P+O) where P = partition cols, O = order cols.
- `VisitorFactory`: Returns a new `List` of 6 freshly-constructed visitor instances on every `expressionVisitors()` call.
- `ColumnLevelLineageBuilder`: Stores `exprDependencies` in `HashMap<ExprId, Set<Dependency>>`; `findDependentInputs` uses a `LinkedList` for duplicate-check with O(N) `.contains()`.
- `ColumnLevelLineageVisitor` (shared interface): Clean interface definition; no performance concerns.

---

## Performance Issues Found

### Issue 1: New VisitorFactory and 6 Visitor Objects Allocated on Every Recursive Traversal Step — Severity: HIGH

**Class**: `ExpressionTraverser`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/ExpressionTraverser.java:84`

**Description**: Every call to `copyFor()` (the primary mechanism for recursing into child expressions) invokes `ExpressionTraverser.of(...)`, which always executes `new VisitorFactory()`. Inside `traverse()`, `visitorFactory.expressionVisitors()` is then called, which constructs a new `Arrays.asList(new AliasVisitor(), new CaseWhenVisitor(), new IfVisitor(), new CoalesceVisitor(), new AggregateExpressionVisitor(), new WindowVisitor())` list on every invocation. For a logical plan with D expression-tree depth and B branching factor, this results in O(B^D) allocations of VisitorFactory + list + 6 visitor objects — one per tree node visited.

**Root Cause**: The two `copyFor()` methods both call `ExpressionTraverser.of(expression, ...)` which hardcodes `new VisitorFactory()` (line 84). The `VisitorFactory` is stateless: none of the six visitors hold any instance state; they are pure functions. There is therefore no reason to create new instances. The one static singleton `visitorFactory` in `ExpressionDependencyCollector` (line 19) is correctly shared, but `ExpressionTraverser` does not follow the same pattern.

**Code Evidence**:
```java
// ExpressionTraverser.java:78-85
public static ExpressionTraverser of(
    Expression expression,
    ExprId outputExpressionId,
    TransformationInfo transformationInfo,
    ColumnLevelLineageBuilder builder) {
  return new ExpressionTraverser(
      expression, outputExpressionId, transformationInfo, builder, new VisitorFactory()); // NEW EACH TIME
}

// ExpressionTraverser.java:87-98
public ExpressionTraverser copyFor(Expression expression) {
  return ExpressionTraverser.of(  // calls the above, which creates new VisitorFactory()
      expression, this.outputExpressionId, this.transformationInfo, this.builder);
}

public ExpressionTraverser copyFor(Expression expression, TransformationInfo transformationInfo) {
  return ExpressionTraverser.of(  // same
      expression, this.outputExpressionId, ...);
}

// VisitorFactory.java:49-58
List<ExpressionVisitor> expressionVisitors() {
  return Collections.unmodifiableList(
      Arrays.asList(
          new AliasVisitor(),          // new object
          new CaseWhenVisitor(),       // new object
          new IfVisitor(),             // new object
          new CoalesceVisitor(),       // new object
          new AggregateExpressionVisitor(), // new object
          new WindowVisitor()));       // new object
}
```

For a `COALESCE(c1, CASE WHEN c2 THEN c3 ELSE c4 END, c5)` — just 3 levels deep with 3 branches — this already creates dozens of VisitorFactory + visitor list allocations.

**Recommendation**: Make `VisitorFactory` a singleton or make the visitor list a static constant. Pass the `VisitorFactory` from the original `ExpressionTraverser` through every `copyFor()` call rather than allocating a new one. The simplest fix is to change `copyFor()` to pass `this.visitorFactory` instead of calling `ExpressionTraverser.of()`:

```java
// Replace the two copyFor() methods with:
public ExpressionTraverser copyFor(Expression expression) {
  return new ExpressionTraverser(
      expression, this.outputExpressionId, this.transformationInfo, this.builder, this.visitorFactory);
}
public ExpressionTraverser copyFor(Expression expression, TransformationInfo transformationInfo) {
  return new ExpressionTraverser(
      expression, this.outputExpressionId, this.transformationInfo.merge(transformationInfo),
      this.builder, this.visitorFactory);
}
```

Alternatively, make `VisitorFactory.expressionVisitors()` return a static immutable list.

---

### Issue 2: CoalesceVisitor Double-Traverses Every Child Sub-Tree — Severity: MEDIUM

**Class**: `CoalesceVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/expression/CoalesceVisitor.java:36-41`

**Description**: For `COALESCE(e1, e2, ..., eN)`, each child expression `e_i` is traversed exactly twice: once as an indirect/conditional dependency and once as a direct dependency. For a `COALESCE` with N children, this means 2N full sub-tree traversals. If any child is itself a complex expression (e.g., a sub-`COALESCE`, a `CASE WHEN`, or an aggregate), the sub-tree is walked twice in full. The intent behind the design is correct — each argument to COALESCE participates in a null-check (indirect) and potentially provides the value (direct) — but at the expression-ID level, the indirect relationship can be registered by adding a dependency edge without re-walking the full sub-tree.

**Root Cause**: `traverser.copyFor(e).traverse()` unconditionally re-enters the recursive tree walk for every child, once per semantic role. There is no memoization of which `ExprId` leaves were discovered during the indirect walk, so the entire sub-tree is re-descended for the direct walk.

**Code Evidence**:
```java
// CoalesceVisitor.java:36-41
ScalaConversionUtils.fromSeq(expr.children())
    .forEach(
        e -> {
          traverser.copyFor(e, TransformationInfo.indirect(CONDITIONAL)).traverse(); // full walk
          traverser.copyFor(e).traverse();   // full walk again — same sub-tree
        });
```

For `COALESCE(a, b, c, d, e)` where each argument is a 10-node expression, this produces 2 * 5 * 10 = 100 node visits instead of 50. For a `COALESCE` of expressions that are themselves `COALESCE` expressions (common in ETL null-chain patterns), the work grows multiplicatively across levels.

**Recommendation**: Collect the set of `ExprId` leaf nodes discovered during a single traversal and register both dependency types against those IDs, avoiding the second full descent. Alternatively, only register the indirect edge at the `COALESCE` level (not via sub-tree traversal) and perform a single direct traversal, since the semantic null-check can be captured without fully re-walking.

---

### Issue 3: Uncached Reflection Lookup on Every AggregateExpression — Severity: MEDIUM

**Class**: `AggregateExpressionVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/column/visitors/expression/AggregateExpressionVisitor.java:40`

**Description**: On every call to `apply()`, the visitor calls `MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId")` to determine whether this is standard Spark or Databricks runtime. `MethodUtils.getAccessibleMethod` performs a reflective method scan of the class and its superclass hierarchy. This check is purely environmental — the answer is determined at JVM startup and never changes during the lifetime of the Spark job. Yet it is re-executed for every `AggregateExpression` node in every expression tree of every operator in the plan.

**Root Cause**: The result of `MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId")` is not cached in a static field. Apache Commons Lang's `MethodUtils` does maintain an internal cache keyed on `(class, methodName, paramTypes)`, so repeat calls within the same JVM are not as expensive as the first cold call — but the cache lookup, synchronization, and null check still happen on every `apply()` invocation, and the `invokeMethod` variant on line 44 also uses reflection on the hot path rather than a direct method reference.

**Code Evidence**:
```java
// AggregateExpressionVisitor.java:40-51
if (MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId") != null) {
    // Standard Spark path: direct method call
    traverser.addDependency(expr.resultId(), TransformationInfo.aggregation());
} else {
    try {
        Seq<ExprId> resultIds = (Seq<ExprId>) MethodUtils.invokeMethod(expr, "resultIds");
        // ... iterate
    } catch (Exception e) {
        log.warn("Failed extracting resultIds from AggregateExpression", e);
    }
}
```

On a plan with 50 aggregate expressions, this performs 50 reflective class introspections. On Databricks, it also invokes `MethodUtils.invokeMethod` reflectively for each aggregate instead of using a direct accessor.

**Recommendation**: Cache the boolean result in a static final field, determined once at class-loading time:
```java
private static final boolean HAS_RESULT_ID =
    MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId") != null;
```
Then use `if (HAS_RESULT_ID)` in `apply()`. For the Databricks path, consider caching a `MethodHandle` or `Method` object to avoid per-call reflective dispatch overhead.

---

### Issue 4: `findDependentInputs` Uses O(N) List.contains() for Duplicate Suppression — Severity: LOW

**Class**: `ColumnLevelLineageBuilder`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java:285-306`

**Description**: `findDependentInputs()` (called at build time from `buildFields()`) uses a `LinkedList<Dependency>` named `dependentInputs` as its "visited" set. On line 298, new candidate dependencies are filtered with `.filter(dependency -> !dependentInputs.contains(dependency))`. `LinkedList.contains()` is O(N) on every check, making each BFS iteration O(N * |frontier|) rather than O(|frontier|). In wide expression graphs (many unique `ExprId`s) this becomes a performance bottleneck at output assembly time.

**Root Cause**: `LinkedList` was chosen where a `HashSet` should be used for visited-tracking. `Dependency` implements proper `equals` and `hashCode`, so `HashSet<Dependency>` would work correctly.

**Code Evidence**:
```java
// ColumnLevelLineageBuilder.java:285-298
List<Dependency> dependentInputs = new LinkedList<>();  // O(N) contains
// ...
.filter(dependency -> !dependentInputs.contains(dependency)) // O(N) per candidate
```

**Recommendation**: Use a `HashSet<Dependency>` (or maintain a parallel `HashSet` alongside the list if ordering is needed) for the visited-check:
```java
Set<Dependency> visited = new HashSet<>();
visited.add(new Dependency(outputExprId, TransformationInfo.identity()));
List<Dependency> dependentInputs = new LinkedList<>(visited);
// then: .filter(d -> !visited.contains(d)) instead of !dependentInputs.contains(d)
```

---

### Issue 5: Linear Scan in `addOutput()` for Every Output Field Registration — Severity: LOW

**Class**: `ColumnLevelLineageBuilder`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/ColumnLevelLineageBuilder.java:92-95`

**Description**: `addOutput(ExprId, String)` performs `schema.getFields().stream().filter(field -> field.getName().equals(attributeName)).findAny()` — a linear scan through all schema fields. This is called once per output attribute per plan node. For a schema with S fields and a plan with P projection nodes, total cost is O(P * S). For wide tables (S = hundreds of columns), this is non-trivial. Similarly, `getOutputExprIdByFieldName()` (line 139) and `getInputsUsedFor(String)` (line 258) each perform the same O(S) linear scan.

**Root Cause**: The schema fields list is never indexed; there is no name-to-field `Map` built at construction time.

**Code Evidence**:
```java
// ColumnLevelLineageBuilder.java:92-95
schema.getFields().stream()
    .filter(field -> field.getName().equals(attributeName))
    .findAny()
    .ifPresent(field -> outputs.putIfAbsent(field, exprId));
```

**Recommendation**: Build a `Map<String, SchemaDatasetFacetFields>` from schema fields in the constructor and use it for O(1) lookups in `addOutput()`, `getOutputExprIdByFieldName()`, and `getInputsUsedFor(String)`.

---

## Clean Classes

- **`AliasVisitor`**: Correct and minimal. Delegates immediately to the child expression with `identity()` transformation. No performance concerns.
- **`IfVisitor`**: Correctly handles the 3-child structure of `If(predicate, trueValue, falseValue)` in O(1) setup + 3 child traversals. No redundancy.
- **`CaseWhenVisitor`**: Despite iterating `branches` twice (once for conditions, once for values), this is two separate passes over a flat list — O(N) total — with no sub-tree re-traversal. The branching correctly separates indirect (condition) from direct (value) semantics. The Scala `ScalaConversionUtils.fromSeq()` call materializes the branches into a Java `List` once, and the two `.stream()` passes are over that materialized list. This is acceptable.
- **`WindowVisitor`**: Correctly short-circuits `RankLike`/`RowNumberLike` to avoid false column dependencies. Traversal of `windowSpec.children()` is O(P + O + 1) where P = partition columns, O = order columns — linear and correct.
- **`ExpressionVisitor` (interface)**: Clean two-method contract. No issues.
- **`ColumnLevelLineageVisitor` (interface)**: Clean three-method contract for the plan-level visitor. No issues.
- **`TransformationInfo`**: Uses pre-allocated static singletons for all common transformation types (TRANSFORMATION_IDENTITY, INDIRECT maps, etc.), avoiding object creation on hot paths. The `merge()` method is O(1) and avoids new object allocation when the masking flag already matches. Well-optimized.
- **`ColumnLevelLineageBuilder.addDependency()`**: Uses `commonDependencies` (a `HashMap<ExprId, Dependency>`) to intern `Dependency` objects with matching `(ExprId, TransformationInfo)` pairs, reducing object allocation for repeated dependency edges. The `COMPUTED_DEPENDENCY_HARD_LIMIT = 1_000_000` guard prevents unbounded growth.

---

## Spark/Iceberg Internals Investigated

- **`/home/bits/perf-audit/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/conditionalExpressions.scala`**:
  - `CaseWhen.children` is defined as `branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue` — a flat list of all condition and value expressions. `ScalaConversionUtils.fromSeq(expr.branches())` in `CaseWhenVisitor` converts the typed `Seq[(Expression, Expression)]` — the structured branch pairs — rather than the flat `children()`. This is correct design: using `branches()` directly avoids having to infer pairing from the flat children list.
  - `If` extends `TernaryLike[Expression]`, confirming its exactly-three-child structure. `IfVisitor` correctly accesses `.predicate()`, `.trueValue()`, `.falseValue()` directly rather than through `children()` iteration.
  - `CaseWhen` has no depth limit in Spark itself; it is a flat list of branches regardless of nesting at the SQL level. Deep nesting of SQL `CASE WHEN` expressions appears as depth in the overall expression tree, not as deeper branch lists within a single `CaseWhen` node.

- **`/home/bits/perf-audit/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/windowExpressions.scala`**:
  - `WindowExpression` is a binary expression: `left = windowFunction`, `right = windowSpec`.
  - `WindowSpecDefinition.children` is `partitionSpec ++ orderSpec :+ frameSpecification`. `WindowVisitor` calls `expr.windowSpec().children()` which correctly includes all three categories. The `frameSpecification` (a `WindowFrame`) is typically a leaf/structural node — its traversal adds no meaningful lineage columns. This is minor but harmless extra work.
  - `WindowExpression` itself does NOT include the `windowSpec` in its `children` as a visible expression for lineage — it is wrapped in a `WindowSpecDefinition` which is the `right` child. `WindowVisitor` correctly calls `expr.windowSpec().children()` to reach the partition and order expressions.

- **`/home/bits/perf-audit/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/nullExpressions.scala`**:
  - `Coalesce.children` is defined simply as `children: Seq[Expression]` — the constructor parameter. No transformation. Each child is an arbitrary expression. Confirmed that `CoalesceVisitor` correctly calls `expr.children()` to get all arguments.
