# Performance Audit - Group 5: Delta and Databricks Handlers

## Summary

The most severe issue is in `CatalogUtils3.getHandlers()`, which is called up to 4 times per dataset per event, and every call re-instantiates all catalog handler objects and invokes `ClassLoader.loadClass()` on each one (Delta, DatabricksDelta, DatabricksUnity) to check class availability. There is zero caching at any level. A secondary issue is that `DeltaHandler.getDatasetVersion` and `DeltaHandler.getDatasetIdentifier` each call `catalog.loadTable()` independently, resulting in two separate Delta table loads per event. Additionally, `DeltaHandler.getDeltaTableSnapshot` performs two uncached `MethodUtils.getAccessibleMethod` reflection lookups per invocation to handle Delta version differences.

## Classes Audited

- `DeltaHandler`: Catalog handler for open-source Delta Lake — loads table twice, reflective method discovery on every version call
- `DatabricksDeltaHandler`: Thin subclass of `AbstractDatabricksHandler` for Databricks proprietary Delta catalog
- `DatabricksUnityV2Handler`: Thin subclass of `AbstractDatabricksHandler` for Databricks Unity Catalog
- `AbstractDatabricksHandler`: Base handler for Databricks proprietary catalogs — reflective `isPathIdentifier` call per dataset
- `SqlDWDatabricksVisitor`: Visitor for Azure Synapse/SQL DW relations — reflective field scan on every `apply()` call

## Performance Issues Found

### [Uncached Handler List Rebuilt With Class Loading On Every Operation] - Severity: HIGH

**Class**: `CatalogUtils3`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/CatalogUtils3.java:26-48`

**Description**: `getHandlers(OpenLineageContext context)` is a private static method that is never cached. It instantiates new handler objects (including `DeltaHandler`, `DatabricksDeltaHandler`, `DatabricksUnityV2Handler`, `IcebergHandler`, `JdbcHandler`, `V2SessionCatalogHandler`) and then filters them through `CatalogHandler::hasClasses` on every call. Each `hasClasses()` implementation calls `ClassLoader.loadClass()` for a fully qualified class name (e.g., `org.apache.spark.sql.delta.catalog.DeltaCatalog`, `com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog`, `com.databricks.sql.managedcatalog.UnityCatalogV2Proxy`).

For a single dataset with version facet in `DataSourceV2RelationDatasetExtractor.extract`, `getHandlers` is called 4 times:
1. Via `CatalogUtils3.getDatasetIdentifier` (line 48)
2. Via `CatalogUtils3.getDatasetVersion` → `getCatalogHandler` (line 135)
3. Via `CatalogUtils3.getStorageDatasetFacet` → `getCatalogHandler` (line 116)
4. Via `CatalogUtils3.getCatalogDatasetFacet` → `getCatalogHandler` (line 124)

**Root Cause**: The result of handler discovery is never stored. Every public utility method (`getDatasetIdentifier`, `getCatalogHandler`, `getDatasetVersion`, `getStorageDatasetFacet`, `getCatalogDatasetFacet`) independently calls `getHandlers(context)`. Java class loaders do cache loaded classes internally, but the overhead of the `loadClass` call itself and the repeated object instantiation and list construction still add measurable per-event cost, especially when multiplied across all datasets in a query plan.

**Code Evidence**:
```java
// CatalogUtils3.java
private static List<CatalogHandler> getHandlers(OpenLineageContext context) {
    List<CatalogHandler> handlers =
        Arrays.asList(
            new IcebergHandler(context),
            new DeltaHandler(context),
            new DatabricksDeltaHandler(context),   // calls loadClass on each hasClasses()
            new DatabricksUnityV2Handler(context), // calls loadClass on each hasClasses()
            new JdbcHandler(context),
            new V2SessionCatalogHandler());
    return handlers.stream().filter(CatalogHandler::hasClasses).collect(Collectors.toList());
}

// DeltaHandler.java - hasClasses() called per getHandlers() invocation
public boolean hasClasses() {
    try {
        DeltaHandler.class.getClassLoader()
            .loadClass("org.apache.spark.sql.delta.catalog.DeltaCatalog");
        return true;
    } catch (NoClassDefFoundError | Exception e) { }
    return false;
}

// AbstractDatabricksHandler.java - same pattern
public boolean hasClasses() {
    try {
        DeltaHandler.class.getClassLoader().loadClass(databricksClassNameString);
        return true;
    } catch (NoClassDefFoundError | Exception e) { }
    return false;
}
```

**Recommendation**: Cache the filtered handler list as a static field or in `OpenLineageContext`. Since available classes cannot change at runtime, the result is stable after the first check. The simplest fix is to make the list a static field initialized lazily with a `AtomicReference` or to change `getHandlers` to accept a pre-built list stored in `OpenLineageContext`. Alternatively, change `private static List<CatalogHandler> getHandlers` to store the result in a `private static volatile List<CatalogHandler> cachedHandlers` field (with a null-check guard) since classpath availability is fixed for a given JVM invocation.

---

### [Duplicate catalog.loadTable() in DeltaHandler per Event] - Severity: MEDIUM

**Class**: `DeltaHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/DeltaHandler.java:68` and `:132`

**Description**: When processing a Delta dataset, `DeltaHandler.getDatasetIdentifier` (line 68) and `DeltaHandler.getDatasetVersion` (line 132) each call `catalog.loadTable(identifier)` independently. In a typical event where both dataset identifier and version are collected (which is the common case via `DataSourceV2RelationDatasetExtractor.extract`), the Delta catalog is asked to resolve and load the same table twice in sequence. `DeltaCatalog.loadTable` reads table metadata from the Delta transaction log.

**Root Cause**: The two methods are called by separate public API paths in `CatalogUtils3` (`getDatasetIdentifier` and `getDatasetVersion`), and each independently resolves the table without any shared state or intermediate result passing.

**Code Evidence**:
```java
// DeltaHandler.getDatasetIdentifier - first loadTable
public DatasetIdentifier getDatasetIdentifier(...) {
    DeltaCatalog catalog = (DeltaCatalog) tableCatalog;
    Table table = catalog.loadTable(identifier);  // line 68: first load
    ...
}

// DeltaHandler.getDatasetVersion - second loadTable on same table
public Optional<String> getDatasetVersion(...) {
    DeltaCatalog deltaCatalog = (DeltaCatalog) tableCatalog;
    Table table = deltaCatalog.loadTable(identifier);  // line 132: second load
    if (table instanceof DeltaTableV2) {
        DeltaTableV2 deltaTable = (DeltaTableV2) table;
        Optional<Snapshot> snapshot = getDeltaTableSnapshot(deltaTable);
        ...
    }
}
```

**Recommendation**: Pass the already-loaded `Table` object between methods, or consolidate dataset identifier and version extraction into a single method that calls `loadTable` once. Alternatively, cache the loaded table in `OpenLineageContext` keyed by `(catalog, identifier)` for the duration of a single event processing cycle.

---

### [Uncached Reflection Method Discovery in getDeltaTableSnapshot] - Severity: MEDIUM

**Class**: `DeltaHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/DeltaHandler.java:155-169`

**Description**: `getDeltaTableSnapshot` is called on every `getDatasetVersion` call. It uses `MethodUtils.getAccessibleMethod(deltaTable.getClass(), "snapshot")` to check if the Delta version exposes a `snapshot` method, and if not, falls back to `MethodUtils.getAccessibleMethod(deltaTable.getClass(), "initialSnapshot")`. Apache Commons `MethodUtils.getAccessibleMethod` does not cache results — it performs fresh reflection lookups (traversing the class hierarchy) on every call.

**Root Cause**: The method was written to handle two different Delta API versions (`snapshot` in Delta < 3, `initialSnapshot` in Delta >= 3). The version of Delta in use at runtime is fixed, so the method-existence check never changes outcome, yet it is repeated on every dataset version extraction.

**Code Evidence**:
```java
private Optional<Snapshot> getDeltaTableSnapshot(DeltaTableV2 deltaTable) {
    // Reflection lookup on every call - not cached
    if (MethodUtils.getAccessibleMethod(deltaTable.getClass(), "snapshot") != null) {
        try {
            return Optional.of((Snapshot) MethodUtils.invokeMethod(deltaTable, "snapshot"));
        } catch (...) { }
    } else if (MethodUtils.getAccessibleMethod(deltaTable.getClass(), "initialSnapshot") != null) {
        try {
            return Optional.of((Snapshot) MethodUtils.invokeMethod(deltaTable, "initialSnapshot"));
        } catch (...) { }
    }
    return Optional.empty();
}
```

**Recommendation**: Cache the result of the method existence check in a static field. Determine once at class-load time (or on first call) which method name is available and store it in a `static volatile String snapshotMethodName` field. On subsequent calls, invoke directly using the cached name.

---

### [Reflective isPathIdentifier Call Per Dataset in AbstractDatabricksHandler] - Severity: MEDIUM

**Class**: `AbstractDatabricksHandler`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/AbstractDatabricksHandler.java:76-80`

**Description**: In `getDatasetIdentifier`, `AbstractDatabricksHandler` uses `MethodUtils.invokeMethod(tableCatalog, true, "isPathIdentifier", identifier)` via reflection to call a method that exists on Databricks' proprietary catalog class. This reflection invocation happens on every dataset identifier resolution and involves method lookup, accessibility checks, and invocation overhead.

**Root Cause**: The `isPathIdentifier` method is on a Databricks proprietary class that cannot be imported directly at compile time. The reflection is necessary for compatibility, but the method lookup is not cached.

**Code Evidence**:
```java
// AbstractDatabricksHandler.java:76-80
try {
    isPathIdentifier =
        (boolean) MethodUtils.invokeMethod(tableCatalog, true, "isPathIdentifier", identifier);
} catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
    // DO Nothing
}
```

**Recommendation**: Cache the `Method` object for `isPathIdentifier` after the first successful lookup in a `static volatile Method` field. Subsequent calls can invoke the cached `Method` directly, avoiding repeated method search through the class hierarchy. The fallback (catching `NoSuchMethodException`) can still be preserved.

---

### [FieldUtils.getAllFieldsList Scans All Fields Per SqlDW apply() Call] - Severity: MEDIUM

**Class**: `SqlDWDatabricksVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/SqlDWDatabricksVisitor.java:77-80`

**Description**: In the `getName` method (called by `apply` on every matching plan node), `FieldUtils.getAllFieldsList(relation.getClass())` is called to get the complete list of all declared fields in the `SqlDWRelation` class hierarchy. The code then iterates the list to find the field named `tableNameOrSubquery`. `getAllFieldsList` performs a full traversal of the class inheritance hierarchy on every call and is not cached.

The comment in the code acknowledges this is necessary because the field has different naming (`tableNameOrSubquery` vs. `com$databricks$spark$sqldw$SqlDWRelation$$tableNameOrSubquery`) across Spark versions, but the class definition does not change at runtime.

**Root Cause**: The field discovery pattern was written for cross-version compatibility without caching. Since `relation.getClass()` is always `SqlDWRelation`, the class hierarchy and its fields are static.

**Code Evidence**:
```java
private Optional<String> getName(BaseRelation relation) {
    String tableName = "";
    try {
        // getAllFieldsList traverses the entire class hierarchy - uncached
        List<Field> relationFields = FieldUtils.getAllFieldsList(relation.getClass());
        for (Field f : relationFields) {
            if (TABLE_FIELD_NAME.equals(f.getName())) {
                tableName = (String) FieldUtils.readField(relation, TABLE_FIELD_NAME, true);
            }
        }
    } catch (IllegalAccessException | IllegalArgumentException e) { ... }
    ...
}
```

**Recommendation**: Cache the discovered `Field` object in a `static volatile Field tableNameField` that is resolved once on first use. Use a `null` sentinel or `AtomicReference` to handle the case where the field is not found. On subsequent calls, use the cached `Field` directly via `field.get(relation)`.

---

### [hasSqlDWDatabricksClasses() Called Per Visitor List Build] - Severity: LOW

**Class**: `SqlDWDatabricksVisitor`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/SqlDWDatabricksVisitor.java:49-58`

**Description**: `hasSqlDWDatabricksClasses()` calls `ClassLoader.loadClass("com.databricks.spark.sqldw.SqlDWRelation")` every time it is called. It is invoked from `BaseVisitorFactory.getBaseCommonVisitors`, which is called from `BaseVisitorFactory.getInputVisitors` and `BaseVisitorFactory.getOutputVisitors`. Since `OpenLineageRunEventBuilder` is constructed per SQL execution context (once per SQL query), this means `hasSqlDWDatabricksClasses` is called twice per query (for input and output visitor list construction).

**Root Cause**: `hasSqlDWDatabricksClasses()` has no caching. The result is a pure function of classpath contents, which cannot change during execution.

**Code Evidence**:
```java
// SqlDWDatabricksVisitor.java:49-57
public static boolean hasSqlDWDatabricksClasses() {
    try {
        SqlDWDatabricksVisitor.class.getClassLoader().loadClass(DATABRICKS_CLASS_NAME);
        return true;
    } catch (NoClassDefFoundError | Exception e) { }
    return false;
}

// BaseVisitorFactory.java:54-56 - called per visitor list build
if (SqlDWDatabricksVisitor.hasSqlDWDatabricksClasses()) {
    list.add(new SqlDWDatabricksVisitor(context, factory));
}
```

**Recommendation**: Cache the result in a `static final boolean` field initialized at class-load time:
```java
private static final boolean HAS_SQLDW_CLASSES = checkHasSqlDWDatabricksClasses();

private static boolean checkHasSqlDWDatabricksClasses() {
    try {
        SqlDWDatabricksVisitor.class.getClassLoader().loadClass(DATABRICKS_CLASS_NAME);
        return true;
    } catch (NoClassDefFoundError | Exception e) { return false; }
}

public static boolean hasSqlDWDatabricksClasses() {
    return HAS_SQLDW_CLASSES;
}
```

The same pattern applies to `DeltaHandler.hasClasses()`, `AbstractDatabricksHandler.hasClasses()`, and `DatasetVersionDatasetFacetUtils.hasDeltaClasses()`.

---

### [DatasetVersionDatasetFacetUtils.hasDeltaClasses() Called Per LogicalRelation] - Severity: LOW

**Class**: `DatasetVersionDatasetFacetUtils`
**Location**: `/home/bits/perf-audit/OpenLineage/integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/utils/DatasetVersionDatasetFacetUtils.java:61` and `:70-80`

**Description**: `hasDeltaClasses()` calls `ClassLoader.loadClass("org.apache.spark.sql.delta.files.TahoeLogFileIndex")` on every invocation of `extractVersionFromLogicalRelation`, which is called per `LogicalRelation` node visited. The method is `protected static` (not `private`), suggesting it was intended to be overridable for testing, but in production it has no caching.

**Root Cause**: No caching of the class existence check.

**Code Evidence**:
```java
// Called on every LogicalRelation (line 61)
if (hasDeltaClasses() && fsRelation.location() instanceof TahoeLogFileIndex) {

// hasDeltaClasses() has no caching (lines 70-80)
protected static boolean hasDeltaClasses() {
    try {
        DatasetVersionDatasetFacetUtils.class.getClassLoader()
            .loadClass("org.apache.spark.sql.delta.files.TahoeLogFileIndex");
        return true;
    } catch (NoClassDefFoundError | Exception e) { }
    return false;
}
```

**Recommendation**: Cache as a `private static final boolean DELTA_CLASSES_AVAILABLE` field initialized at class load time.

## Clean Classes

- **`DatabricksDeltaHandler`**: This class is a thin wrapper over `AbstractDatabricksHandler` with only `getStorageDatasetFacet`, `getCatalogDatasetFacet`, and `getName` overrides. All three methods are simple object construction with no I/O, loops, or external calls. The class itself is clean; any performance issues are inherited from `AbstractDatabricksHandler`.

- **`DatabricksUnityV2Handler`**: Same assessment as `DatabricksDeltaHandler` — only lightweight facet-construction overrides. Clean in isolation.

- **`DatabricksEnvironmentFacetBuilder.isDatabricksRuntime()`**: Uses `System.getenv().containsKey("DATABRICKS_RUNTIME_VERSION")`, which is an in-process environment map lookup. Acceptable.

## Spark/Iceberg Internals Investigated

- **`org.apache.commons.lang3.reflect.MethodUtils` (Apache Commons Lang3)**: `MethodUtils.getAccessibleMethod` does not maintain a cache. It performs a fresh traversal of the declared methods of the class and its superclasses on every call. `MethodUtils.invokeMethod` similarly performs a fresh method lookup before invocation. This confirms that repeated calls to these methods in hot paths (like `getDeltaTableSnapshot`) are not self-optimizing.

- **`org.apache.commons.lang3.reflect.FieldUtils` (Apache Commons Lang3)**: `FieldUtils.getAllFieldsList` does not cache. It traverses the class hierarchy using `getDeclaredFields()` (which itself is a JVM call) on every invocation. There is no internal memoization in the library.

- **`DeltaCatalog.loadTable`** (Delta Lake): Loading a Delta table involves reading the Delta log directory structure from the filesystem (or cache if DeltaLog has it in-memory). While DeltaLog maintains an in-memory log cache per path, calling `loadTable` twice for the same identifier still involves catalog path resolution and `DeltaLog.forTable` lookup on both calls. On first access within a task, this can involve filesystem metadata reads.

- **`ClassLoader.loadClass`** (JVM): After a class is loaded once, the JVM classloader caches it. Subsequent `loadClass` calls for the same name return quickly from the cache. However, the overhead is not zero — it involves classloader delegation, synchronization on the classloader lock, and a hash table lookup. When called 4+ times per dataset across multiple catalog handlers, the aggregate cost in high-throughput streaming or large batch scenarios with many datasets is non-trivial compared to a one-time boolean flag.
