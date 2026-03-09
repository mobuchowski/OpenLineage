# Performance Audit - Group 14: RDD, Streaming, AppendData, SubqueryAlias, View, Cosmos, Kusto

## Summary

The most significant performance issue in this group is in `KustoRelationVisitor.isKustoClass`, which calls `ClassLoader.loadClass` on every single `LogicalRelation` node encountered during plan traversal, with no caching of the result. A secondary concern is in `FileScanRDDExtractor` which iterates over every `PartitionedFile` within every `FilePartition` to extract parent paths, scaling linearly with total file count across all partitions. The remaining classes are largely clean: `CosmosHandler.hasClasses` is called at static initialization time (once per JVM), `InMemoryRelationInputDatasetBuilder` correctly looks up plan metadata from `CacheManager` without re-executing cached data, and `SubqueryAliasInputDatasetBuilder`'s self-referential recursion is bounded by plan depth and protected by `VisitedNodes` for already-visited subplans.

## Classes Audited

- `AbstractRDDNodeVisitor`: Base class for RDD visitors; delegates to `PlanUtils.findDatasetIdentifiers` which routes through `RddDatasetInfoExtractor`
- `ExternalRDDVisitor`: Matches `ExternalRDD` nodes; uses `Rdds.findFileLikeRdds` then `findInputDatasets`
- `LogicalRDDVisitor`: Matches `LogicalRDD` nodes; calls `Rdds.flattenRDDs` (with cycle detection) then `findFileLikeRdds`
- `HadoopRDDInputDatasetBuilder`: Extracts paths from `HadoopRDD`/`NewHadoopRDD` via `FileInputFormat.getInputPaths`; calls `getFileStatus` per unique path
- `AppendDataDatasetBuilder` (shared/spark3): Delegates to existing output visitors for the `AppendData.table()` node; no independent overhead
- `SubqueryAliasInputDatasetBuilder`: Recursively unwraps `SubqueryAlias` chains using a local builder list that includes `this`; depth-bounded by plan structure
- `SubqueryAliasOutputDatasetBuilder`: Delegates directly to global output visitors for the alias child; clean
- `InMemoryRelationInputDatasetBuilder`: Looks up the originating `LogicalPlan` from `CacheManager` via reflection, then runs `plan.collect()` over it
- `ViewInputDatasetBuilder`: Strips one `Project` wrapper and delegates to the global visitor list; clean for non-nested-view cases
- `CosmosHandler`: Parses dataset identifier from the relation table name string; `hasClasses` called once at static init time
- `KustoRelationVisitor`: Uses `ClassLoader.loadClass` on every `isDefinedAt` call; uses `FieldUtils.readField` (reflection) to extract metadata

## Performance Issues Found

### [KustoRelationVisitor Calls loadClass on Every Plan Node] - Severity: HIGH

**Class**: `KustoRelationVisitor`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/KustoRelationVisitor.java:50-57`
**Description**: `isDefinedAt(LogicalPlan)` calls `isKustoClass(plan)`, which calls `Thread.currentThread().getContextClassLoader().loadClass(KUSTO_CLASS_NAME)` on every invocation. Since `isDefinedAt` is called for every `LogicalRelation` node in the query plan (the outer visitor dispatch calls it for each plan node), this means a `loadClass` call per `LogicalRelation` in the plan, for every lineage event.
**Root Cause**: There is no per-instance or per-JVM caching of the class lookup result. The class reference `c` is a local variable, discarded immediately after use. The method `hasKustoClasses()` does the correct pattern (try/catch with early return) but is only used to gate whether the visitor is registered at startup — `isKustoClass` independently re-does the `loadClass` on every plan dispatch without consulting any cached result.
**Code Evidence**:
```java
protected boolean isKustoClass(LogicalPlan plan) {
    try {
        Class c = Thread.currentThread().getContextClassLoader().loadClass(KUSTO_CLASS_NAME);
        return (plan instanceof LogicalRelation
            && c.isAssignableFrom(((LogicalRelation) plan).relation().getClass()));
    } catch (Exception e) {
        // swallow - not a kusto class
    }
    return false;
}

@Override
public boolean isDefinedAt(LogicalPlan plan) {
    return isKustoClass(plan); // called for every LogicalRelation node
}
```
**Recommendation**: Cache the loaded `Class` object as a static final field at class-load time (using the same try/catch pattern as `hasKustoClasses()`). If the class is not on the classpath, store `null` and return `false` early. This reduces the per-event cost from O(plan_LogicalRelation_nodes * loadClass) to O(1) lookup per node:

```java
private static final Class<?> KUSTO_CLASS;
static {
    Class<?> c = null;
    try { c = Class.forName(KUSTO_CLASS_NAME); } catch (Exception ignored) {}
    KUSTO_CLASS = c;
}

protected boolean isKustoClass(LogicalPlan plan) {
    return KUSTO_CLASS != null
        && plan instanceof LogicalRelation
        && KUSTO_CLASS.isAssignableFrom(((LogicalRelation) plan).relation().getClass());
}
```

---

### [FileScanRDDExtractor Iterates All Files Across All Partitions] - Severity: MEDIUM

**Class**: `RddDatasetInfoExtractor.FileScanRDDExtractor`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/RddDatasetInfoExtractor.java:168-181`
**Description**: `FileScanRDDExtractor.extract` streams all `FilePartition` objects from `rdd.filePartitions()` and then flat-maps over every `PartitionedFile` within each partition to extract parent directory paths. For a dataset with thousands of partitions, each containing many files, this is O(total_file_count). The `getParent()` operation is a pure in-memory string operation (no I/O), but the sheer volume of object iteration scales with total files across the entire scan.
**Root Cause**: The extractor aims for correctness (finding all unique dataset directory paths), but does not short-circuit once a directory path is identified. For a typical partitioned dataset where all files live under a small number of root directories, most of the file iteration produces duplicates that are later discarded by `.distinct()` in `PlanUtils.findDatasetIdentifiers`. Deduplication is deferred to the terminal operation instead of being applied eagerly.
**Code Evidence**:
```java
public Stream<DatasetIdentifier> extract(FileScanRDD rdd) {
    return ScalaConversionUtils.fromSeq(rdd.filePartitions()).stream()
        .flatMap((FilePartition fp) -> Arrays.stream(fp.files()))  // O(total_files)
        .map(f -> {
            if ("3.4".compareTo(package$.MODULE$.SPARK_VERSION()) <= 0) {
                return tryExecuteMethod(f, "filePath")
                    .map(o -> tryExecuteMethod(o, "toPath"))
                    .map(o -> (Path) o.get()).get().getParent();
            } else {
                return parentOf(f.filePath());
            }
        })
        .filter(Objects::nonNull)
        .map(PathUtils::fromPath);
    // .distinct() only happens later in PlanUtils.findDatasetIdentifiers
}
```
**Recommendation**: Short-circuit by sampling one file per partition instead of iterating all files. Since files within a single `FilePartition` are grouped by locality and typically share the same root directory, examining the first file of each `FilePartition` is sufficient to identify the dataset location. This reduces O(total_files) to O(num_partitions):

```java
public Stream<DatasetIdentifier> extract(FileScanRDD rdd) {
    return ScalaConversionUtils.fromSeq(rdd.filePartitions()).stream()
        .map(fp -> fp.files().length > 0 ? fp.files()[0] : null)  // first file only
        .filter(Objects::nonNull)
        .map(f -> parentOf(f.filePath()))
        ...
}
```

---

### [DataSourceRDDExtractor Bypasses Spark's RDD Partition Cache] - Severity: LOW

**Class**: `RddDatasetInfoExtractor.DataSourceRDDExtractor`
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/RddDatasetInfoExtractor.java:284-296`
**Description**: `extractInputPartitions` calls `rdd.getPartitions()` directly (the overridden protected Scala method, which compiles to a public method in JVM bytecode). Spark's `RDD` class provides a `final def partitions` accessor that caches the result of `getPartitions` in a `@volatile partitions_` field. By calling `getPartitions()` directly from Java, OpenLineage bypasses this cache and causes `DataSourceRDD.getPartitions` to re-execute its `inputPartitions.zipWithIndex.map { case (inputPartitions, index) => new DataSourceRDDPartition(index, inputPartitions) }.toArray` allocation each time, creating N new `DataSourceRDDPartition` objects per call.
**Root Cause**: Java callers interop with Scala: Scala's `protected def getPartitions` compiles to a public JVM method accessible from Java, while `final def partitions` is the intended public-facing cached accessor. The code uses the lower-level uncached method.
**Code Evidence**:
```java
// Spark 3.3+
inputPartitions = Arrays.stream(rdd.getPartitions())  // bypasses RDD.partitions() caching
    .filter(p -> p instanceof DataSourceRDDPartition)
    ...
// Spark < 3.3
inputPartitions = Arrays.stream(rdd.getPartitions())  // same issue
    ...
```
**Recommendation**: Use `rdd.partitions()` instead of `rdd.getPartitions()` to use Spark's internal partition caching. In Java, this is the correct public API. This eliminates repeated `DataSourceRDDPartition` object allocation when the same RDD is inspected more than once within an event.

---

### [HadoopRDD Path Resolution Makes Live Filesystem getFileStatus Call] - Severity: LOW

**Class**: `RddDatasetInfoExtractor.HadoopRDDExtractor` / `RddDatasetInfoExtractor.NewHadoopRDDExtractor` (via `PlanUtils.getDirectoryPaths`)
**Location**: `integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/PlanUtils.java:280`
**Description**: `PlanUtils.getDirectoryPaths` is called by both `HadoopRDDExtractor` and `NewHadoopRDDExtractor`. For each unique path not yet in `normalizedPaths`, it calls `p.getFileSystem(hadoopConf).getFileStatus(p).isFile()` to determine whether the path is a file or a directory. This is a live filesystem RPC call (HDFS `getFileStatus` or equivalent object store HEAD request) that adds network latency to lineage extraction at job start/end time.
**Root Cause**: The check is needed to normalize file-level paths to their parent directory. For `HadoopRDD`, the configured input paths from `FileInputFormat.getInputPaths` are typically directories, so this `getFileStatus` call usually returns `false` (path is not a file) — but the RPC still occurs for each unique path. `FileScanRDDExtractor` demonstrates the clean alternative: it directly calls `.getParent()` without any filesystem I/O.
**Code Evidence**:
```java
private static Path getDirectoryPath(Path p, Configuration hadoopConf) {
    try {
        if (p.getFileSystem(hadoopConf).getFileStatus(p).isFile()) {  // filesystem RPC per path
            return p.getParent();
        } else {
            return p;
        }
    } catch (IOException e) {
        log.warn("Unable to get file system for path: {}", e.getMessage());
        return p;
    }
}
```
**Recommendation**: Add a configuration flag to skip the `getFileStatus` check (defaulting to the current behavior for backwards compatibility). In most production deployments, `HadoopRDD` input paths are directories; an opt-in `skipFsCheck` mode could treat all paths as directories and call `.getParent()` only when the path has a file-like suffix. Alternatively, use filesystem scheme heuristics (e.g., object store URIs like `s3://`, `gs://` are always directories at the configured level) to skip the round-trip.

---

### [InMemoryRelationInputDatasetBuilder Does Full Plan Tree Traversal per Cache Hit] - Severity: LOW

**Class**: `InMemoryRelationInputDatasetBuilder`
**Location**: `integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/InMemoryRelationInputDatasetBuilder.java:43-54`
**Description**: When an `InMemoryRelation` node is encountered, the builder first calls `PlanUtils3.getLogicalPlanOf` which performs a linear scan of `CacheManager.cachedData` (accessed via reflection on `SharedState`) to locate the original `LogicalPlan`. It then calls `plan.collect(delegate(...))` — Scala's `TreeNode.collect` — which traverses the entire original plan tree and invokes all registered input dataset builders on every node. For complex original plans with many nodes, and for jobs that reference multiple cached datasets, this is O(cached_entries + original_plan_nodes * num_builders) per `InMemoryRelation` node.
**Root Cause**: The `CacheManager.cachedData` scan is O(N) where N is the number of `spark.catalog.cacheTable()` entries (using `cachedName` equality in a linear stream, not a hash-map lookup). The `plan.collect()` call is an unavoidable full traversal. Both operations use reflection (`FieldUtils.getField`) to access Spark internals.
**Code Evidence**:
```java
public List<OpenLineage.InputDataset> apply(SparkListenerEvent event, InMemoryRelation inMemoryRelation) {
    return PlanUtils3.getLogicalPlanOf(context, inMemoryRelation)  // linear scan of cachedData via reflection
        .map(
            plan ->
                ScalaConversionUtils.fromSeq(
                        plan.collect(   // full traversal of entire original plan tree
                            delegate(context.getInputDatasetQueryPlanVisitors(),
                                     context.getInputDatasetBuilders(), event)))
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
}
```
**Recommendation**: The behavior is functionally necessary. Consider caching the `cachedName -> LogicalPlan` lookup in an event-scoped or context-scoped `Map<String, LogicalPlan>` to avoid redundant `CacheManager` scans if the same `InMemoryRelation` (identified by `cachedName`) appears multiple times in one event's plan. The `plan.collect()` traversal itself cannot easily be avoided.

## Clean Classes

- **`AbstractRDDNodeVisitor`**: Pure delegation class. `findInputDatasets` streams through `PlanUtils.findDatasetIdentifiers` which handles deduplication. No direct performance concern.
- **`ExternalRDDVisitor`**: Calls `Rdds.findFileLikeRdds` (stack-based iterative graph traversal, no recursion risk) then delegates. Clean.
- **`LogicalRDDVisitor`**: Uses `Rdds.flattenRDDs` with a `Set<Integer>` visited-RDD-IDs guard that prevents revisiting the same RDD ID in the dependency graph. Schema resolution uses `PlanUtils.findSchema` (first-match, short-circuiting). Clean.
- **`AppendDataDatasetBuilder` (shared and spark3)**: Both implementations immediately delegate to existing output dataset visitors on `appendData.table()`. Zero additional work beyond delegation overhead.
- **`SubqueryAliasInputDatasetBuilder`**: The self-referential `.add(this)` in the local builder list enables recursive alias unwrapping (SubqueryAlias → SubqueryAlias → ... → base relation). Each recursive step peels exactly one alias layer; depth is bounded by SQL nesting depth (typically single digits in real queries). `VisitedNodes.alreadyVisited` (keyed by `semanticHash()`) prevents re-processing already-resolved subplans within the same event. No unbounded recursion risk in practice.
- **`SubqueryAliasOutputDatasetBuilder`**: Simple single-level delegation to the global output visitors with no self-reference. The `jobNameSuffix` uses only `trimPath` (string operation). Clean.
- **`ViewInputDatasetBuilder`**: Strips at most one `Project` wrapper then delegates to the global visitor context. Does not self-reference. Spark's analyzer resolves nested views before they appear in the analyzed/optimized plan, so View-within-View cycles do not occur in practice. Clean.
- **`CosmosHandler`**: `hasClasses()` is called only once at class-loading time via `CatalogUtils3`'s static field `getRelationHandlers()`. After that, the filtered `relationHandlers` list is static. At query time, `isClass` checks `relation.table().name().contains("com.azure.cosmos.spark.items.")` (pure string operation) and `getDatasetIdentifier` parses the table name with a `String.split`. No HTTP calls; the Cosmos endpoint is parsed from the pre-computed table name string, not fetched from the Azure Cosmos API. Clean.
- **`KustoRelationVisitor.apply`**: Once `isDefinedAt` returns true (the problematic `loadClass` path), `apply` uses `FieldUtils.readField` (reflection) to read `query`, `kustoCoordinates`, `clusterUrl`, and `database` fields from the already-instantiated in-memory relation object. No network calls occur in `apply`. Reflection field access after JVM warmup is fast (JIT-compiled). Clean except for the `isDefinedAt` issue documented above.

## Spark/Iceberg Internals Investigated

- **`/home/bits/perf-audit/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileScanRDD.scala`**: Confirmed `filePartitions` is a `@transient val` constructor parameter (O(1) field access). `getPartitions` returns `filePartitions.toArray`. Each `FilePartition` holds an `Array[PartitionedFile]`, confirming that iterating all files within all partitions is O(total_file_count).
- **`/home/bits/perf-audit/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FilePartition.scala`**: Confirmed `FilePartition` is a simple case class holding `index: Int` and `files: Array[PartitionedFile]`. No lazy evaluation or expensive computation; files are pre-computed at plan construction time.
- **`/home/bits/perf-audit/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceRDD.scala`**: Confirmed `override protected def getPartitions: Array[Partition]` allocates new `DataSourceRDDPartition` objects on every call by calling `.toArray` on the result of `inputPartitions.zipWithIndex.map(...)`. Key finding: Java callers bypass Spark's internal `partitions_` cache by calling `getPartitions()` directly rather than `partitions()`.
- **`/home/bits/perf-audit/spark/core/src/main/scala/org/apache/spark/rdd/RDD.scala`**: Confirmed `final def partitions: Array[Partition]` uses `@volatile partitions_` caching (set on first call, reused thereafter). `protected def getPartitions` is the uncached override. Scala `protected def` compiles to a public JVM method, meaning Java code can call `rdd.getPartitions()` directly and bypass the `partitions()` caching layer.
- **`/home/bits/perf-audit/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala`**: Confirmed `cachedData` is `@transient @volatile private var cachedData = IndexedSeq[CachedData]()` — an `IndexedSeq` (effectively immutable array-backed). `getLogicalPlanOf` scans it with `.stream().filter(...)` — a linear O(N) scan by `cachedName` equality. No hash-map or index lookup is available.
