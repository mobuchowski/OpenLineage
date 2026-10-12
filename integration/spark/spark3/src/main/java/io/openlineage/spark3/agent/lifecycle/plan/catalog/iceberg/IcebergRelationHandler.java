/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.lifecycle.plan.catalog.RelationHandler;
import io.openlineage.spark.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Resolves Iceberg datasets straight from the relation, without going through the catalog that the
 * relation was loaded from.
 *
 * <p>This is required because Iceberg's rewrite actions ({@code rewrite_data_files}, {@code
 * rewrite_position_delete_files}, …) do not write through the table's own catalog. They stage the
 * table in {@code SparkTableCache} under a random key and then read and write through a catalog
 * that Iceberg silently registers for that cache:
 *
 * <ul>
 *   <li>{@code org.apache.iceberg.spark.SparkCachedTableCatalog} ({@code default_cache_iceberg}),
 *       serving {@link SparkTable}s - Iceberg's Spark 3.x and 4.0 modules;
 *   <li>{@code org.apache.iceberg.spark.SparkRewriteTableCatalog} ({@code
 *       default_rewrite_catalog}), serving {@code SparkRewriteTable}s - Iceberg's Spark 4.1 module,
 *       from Iceberg 1.11 on.
 * </ul>
 *
 * <p>Either catalog implements {@link TableCatalog} directly - it is neither a {@code SparkCatalog}
 * nor a {@code SparkSessionCatalog} - so {@link IcebergHandler#isClass(TableCatalog)} rejects it
 * and the whole catalog-based resolution fails, leaving the emitted event with no datasets at all.
 *
 * <p>The relation itself still carries everything needed: {@code relation.table()} wraps the real
 * Iceberg {@link Table}, whose {@code name()} is the fully qualified name assigned by the catalog
 * that originally loaded it (for example {@code prod_catalog.namespace.table}). That is used to
 * find the owning catalog and delegate to {@link IcebergHandler}, so a compaction job reports
 * exactly the same dataset identifier - symlinks included - as a regular write to the same table.
 */
@Slf4j
public class IcebergRelationHandler implements RelationHandler {
  private static final String SPARK_TABLE_CLASS_NAME = "org.apache.iceberg.spark.source.SparkTable";

  /**
   * Served by {@code SparkRewriteTableCatalog}. It extends Iceberg's package-private {@code
   * BaseSparkTable} rather than {@link SparkTable} and does not exist in the Iceberg version this
   * module compiles against, so it is matched by name and unwrapped through its public {@code
   * table()} accessor. Matching the exact class keeps other Iceberg wrappers - changelog or
   * metadata tables, which are not the base table - out of this handler.
   */
  private static final String REWRITE_TABLE_CLASS_NAME =
      "org.apache.iceberg.spark.source.SparkRewriteTable";

  /** The {@code table()} accessor, looked up once per loaded class rather than per relation. */
  private static final ClassValue<Optional<Method>> TABLE_ACCESSOR =
      new ClassValue<Optional<Method>>() {
        @Override
        protected Optional<Method> computeValue(Class<?> type) {
          try {
            return Optional.of(type.getMethod("table"));
          } catch (NoSuchMethodException e) {
            log.warn("{} has no table() accessor; its datasets cannot be resolved", type.getName());
            return Optional.empty();
          }
        }
      };

  /** A table name that can be split into a catalog and a table has at least those two parts. */
  private static final int MIN_NAME_PARTS = 2;

  private final OpenLineageContext context;

  public IcebergRelationHandler(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean hasClasses() {
    try {
      IcebergRelationHandler.class.getClassLoader().loadClass(SPARK_TABLE_CLASS_NAME);
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      log.debug("The iceberg spark runtime is not present");
    }
    return false;
  }

  @Override
  public boolean isClass(DataSourceV2Relation relation) {
    return icebergTable(relation.table()).isPresent();
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(DataSourceV2Relation relation) {
    Table icebergTable =
        icebergTable(relation.table())
            .orElseThrow(
                () ->
                    new UnsupportedCatalogException(
                        String.format(
                            "Relation over %s does not wrap an Iceberg table",
                            relation.table().getClass().getName())));
    return owningCatalog(icebergTable)
        .flatMap(owner -> resolveThroughOwningCatalog(relation, icebergTable, owner))
        .orElseGet(() -> fromTableLocation(icebergTable));
  }

  /**
   * Recovers the catalog that originally loaded the table from the table's fully qualified name.
   * Returns empty when the name cannot be split into catalog and table parts (tables loaded by path
   * are named by their location), when there is no Spark session to look the catalog up in, or when
   * the registered catalog is not a {@link TableCatalog}.
   */
  @Override
  public Optional<OwningCatalog> getOwningCatalog(DataSourceV2Relation relation) {
    return icebergTable(relation.table()).flatMap(this::owningCatalog);
  }

  private Optional<OwningCatalog> owningCatalog(Table icebergTable) {
    if (!context.getSparkSession().isPresent()) {
      return Optional.empty();
    }

    String tableName = icebergTable.name();
    if (tableName == null || tableName.contains("/")) {
      return Optional.empty();
    }

    String[] parts = tableName.split("\\.");
    if (parts.length < MIN_NAME_PARTS) {
      return Optional.empty();
    }

    // Only the catalog segment is taken from the qualified name; the remaining segments are split
    // into namespace and table exactly as Spark parses a multipart identifier. The relation's own
    // identifier is deliberately not used - for the rewrite-catalog writes this handler exists for,
    // it is the SparkTableCache key rather than the table's real identifier.
    Identifier identifier =
        Identifier.of(Arrays.copyOfRange(parts, 1, parts.length - 1), parts[parts.length - 1]);

    return SparkSessionUtils.catalog(context.getSparkSession().get(), parts[0])
        .filter(TableCatalog.class::isInstance)
        .map(catalog -> OwningCatalog.of((TableCatalog) catalog, identifier));
  }

  /**
   * The Iceberg table wrapped by a {@link SparkTable} or a {@code SparkRewriteTable}; empty for any
   * other table, including a {@link SparkTable} that wraps none.
   */
  static Optional<Table> icebergTable(org.apache.spark.sql.connector.catalog.Table sparkTable) {
    if (sparkTable instanceof SparkTable) {
      return Optional.ofNullable(((SparkTable) sparkTable).table());
    }
    if (sparkTable == null || !REWRITE_TABLE_CLASS_NAME.equals(sparkTable.getClass().getName())) {
      return Optional.empty();
    }
    return TABLE_ACCESSOR
        .get(sparkTable.getClass())
        .flatMap(accessor -> invokeTableAccessor(accessor, sparkTable));
  }

  private static Optional<Table> invokeTableAccessor(
      Method accessor, org.apache.spark.sql.connector.catalog.Table sparkTable) {
    try {
      Object table = accessor.invoke(sparkTable);
      return table instanceof Table ? Optional.of((Table) table) : Optional.empty();
    } catch (IllegalAccessException | InvocationTargetException e) {
      log.debug("Could not unwrap iceberg table from {}", sparkTable.getClass().getName(), e);
      return Optional.empty();
    }
  }

  /**
   * Last resort when the owning catalog cannot be recovered. A table with no usable location leaves
   * nothing to identify it by, so this raises {@link UnsupportedCatalogException} - the one
   * exception the caller of {@link RelationHandler} is contracted to handle - rather than letting
   * {@code new Path(null)} throw {@link IllegalArgumentException} out of the handler.
   */
  private DatasetIdentifier fromTableLocation(Table icebergTable) {
    String location = icebergTable.location();
    if (location == null || location.trim().isEmpty()) {
      throw new UnsupportedCatalogException(
          String.format("Iceberg table %s has no location to identify it by", icebergTable.name()));
    }
    return PathUtils.fromPath(new Path(location));
  }

  /**
   * Re-runs the regular {@link IcebergHandler} resolution against the catalog that owns the table.
   * Returns empty when that catalog is not one the {@link IcebergHandler} supports, in which case
   * the caller falls back to the table location.
   */
  private Optional<DatasetIdentifier> resolveThroughOwningCatalog(
      DataSourceV2Relation relation, Table icebergTable, OwningCatalog owner) {
    try {
      return Optional.of(
          CatalogUtils.getDatasetIdentifier(
              context, owner.getCatalog(), owner.getIdentifier(), relation.table().properties()));
    } catch (UnsupportedCatalogException e) {
      log.debug("Catalog owning iceberg table {} is unsupported", icebergTable.name());
      return Optional.empty();
    } catch (Exception | LinkageError e) {
      // hasClasses() only proves SparkTable loads; the resolution above reaches deeper into the
      // Iceberg and Spark APIs, where a version mismatch surfaces as a linkage error.
      log.debug("Could not resolve dataset of iceberg table {}", icebergTable.name(), e);
      return Optional.empty();
    }
  }

  @Override
  public String getName() {
    return "iceberg";
  }
}
