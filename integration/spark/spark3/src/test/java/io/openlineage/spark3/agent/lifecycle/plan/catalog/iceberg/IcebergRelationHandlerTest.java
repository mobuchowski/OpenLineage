/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.spark.agent.lifecycle.DatasetBuilderFactory;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.lifecycle.plan.catalog.RelationHandler;
import io.openlineage.spark.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkCachedTableCatalog;
import org.apache.iceberg.spark.source.SparkRewriteTable;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import scala.PartialFunction;

class IcebergRelationHandlerTest {

  private static final String TABLE_LOCATION = "file:/tmp/wh/db/tbl";
  private static final String TABLE_PATH = "/tmp/wh/db/tbl";
  private static final String QUALIFIED_NAME = "c.db.tbl";

  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final IcebergRelationHandler handler = new IcebergRelationHandler(context);

  @Test
  void testHasClasses() {
    assertThat(handler.hasClasses()).isTrue();
  }

  @Test
  void testIsClassForIcebergRelation() {
    assertThat(handler.isClass(relationOf(TABLE_LOCATION, QUALIFIED_NAME))).isTrue();
  }

  /**
   * Iceberg 1.11's Spark 4.1 module serves rewrite actions a SparkRewriteTable, not a SparkTable.
   */
  @Test
  void testIsClassForRewriteTableRelation() {
    assertThat(handler.isClass(rewriteRelationOf(TABLE_LOCATION, QUALIFIED_NAME))).isTrue();
  }

  /**
   * Only SparkRewriteTable is unwrapped reflectively. Another table exposing a {@code table()}
   * accessor - an Iceberg changelog or metadata table, say - is not the base table.
   */
  @Test
  void testIsClassRejectsOtherTablesWithTableAccessor() {
    Table icebergTable = mock(Table.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(relation.table()).thenReturn(new WrapperWithTableAccessor(icebergTable));

    assertThat(handler.isClass(relation)).isFalse();
  }

  /** Used to be claimed and then fail with a NullPointerException in getDatasetIdentifier. */
  @Test
  void testIsClassRejectsSparkTableWithoutIcebergTable() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(relation.table()).thenReturn(mock(SparkTable.class));

    assertThat(handler.isClass(relation)).isFalse();
  }

  @Test
  void testIsClassForNonIcebergRelation() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);

    assertThat(handler.isClass(relation)).isFalse();
  }

  /**
   * With no Spark session there is no catalog manager to recover the owning catalog from, so the
   * handler falls back to the table location - which is still the correct dataset.
   */
  @Test
  void testGetDatasetIdentifierFallsBackToTableLocation() {
    when(context.getSparkSession()).thenReturn(Optional.empty());

    DatasetIdentifier di = handler.getDatasetIdentifier(relationOf(TABLE_LOCATION, QUALIFIED_NAME));

    assertThat(di.getName()).isEqualTo(TABLE_PATH);
    assertThat(di.getNamespace()).isEqualTo("file");
  }

  /** A table loaded without a catalog is named by its location, which cannot be split up. */
  @Test
  void testGetDatasetIdentifierForTableWithoutCatalog() {
    when(context.getSparkSession()).thenReturn(Optional.empty());

    DatasetIdentifier di = handler.getDatasetIdentifier(relationOf(TABLE_LOCATION, TABLE_LOCATION));

    assertThat(di.getName()).isEqualTo(TABLE_PATH);
  }

  @Test
  void testGetName() {
    assertThat(handler.getName()).isEqualTo("iceberg");
  }

  /** Guards the registration - an unregistered handler is dead code. */
  @Test
  void testHandlerIsRegisteredForIcebergRelations() {
    OpenLineageContext contributingContext = contextContributing(handler);
    when(context.getSparkSession()).thenReturn(Optional.empty());

    DatasetIdentifier di =
        CatalogUtils.getDatasetIdentifierFromRelation(
            contributingContext, relationOf("file:/tmp/wh/db/registered", "c.db.registered"));

    assertThat(di.getName()).isEqualTo("/tmp/wh/db/registered");
  }

  /** A relation that no handler recognises must still raise, so the caller can log and skip. */
  @Test
  void testUnknownRelationStillThrows() {
    OpenLineageContext contributingContext = contextContributing(handler);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(relation.table()).thenReturn(mock(org.apache.spark.sql.connector.catalog.Table.class));

    assertThat(handler.isClass(relation)).isFalse();
    assertThatThrownBy(
            () -> CatalogUtils.getDatasetIdentifierFromRelation(contributingContext, relation))
        .isInstanceOf(UnsupportedCatalogException.class);
  }

  @Test
  void testIcebergCatalogHandlerStillRejectsCachedCatalog() {
    // the reason this relation handler exists in the first place
    TableCatalog cached = mock(SparkCachedTableCatalog.class);
    assertThat(new IcebergHandler(context).isClass(cached)).isFalse();
  }

  /**
   * The path this handler exists for. The owning catalog is recovered from the table's qualified
   * name and the identifier resolved through it, so the result is whatever the regular {@link
   * IcebergHandler} would have produced for a plain write - symlink included. Asserting on the
   * symlink is what separates this from the location fallback, which resolves to the same primary
   * name from the table location alone and would otherwise satisfy the assertion on its own.
   */
  @Test
  void testGetDatasetIdentifierResolvesThroughOwningCatalog() {
    DatasetIdentifier throughCatalog =
        new DatasetIdentifier(TABLE_PATH, "file")
            .withSymlink("db.tbl", "hive://metastore", SymlinkType.TABLE);
    TableCatalog owning = mock(TableCatalog.class);
    withSessionCatalog(context, "c", owning);

    try (MockedStatic<CatalogUtils> catalogUtils =
        mockStatic(CatalogUtils.class, Mockito.CALLS_REAL_METHODS)) {
      catalogUtils
          .when(
              () ->
                  CatalogUtils.getDatasetIdentifier(
                      Mockito.eq(context),
                      Mockito.eq(owning),
                      Mockito.eq(Identifier.of(new String[] {"db"}, "tbl")),
                      Mockito.any()))
          .thenReturn(throughCatalog);

      DatasetIdentifier di =
          handler.getDatasetIdentifier(relationOf(TABLE_LOCATION, QUALIFIED_NAME));

      assertThat(di.getName()).isEqualTo(TABLE_PATH);
      assertThat(di.getSymlinks())
          .as("the identifier must come from the owning catalog, not the table location")
          .isNotEmpty();
    }
  }

  /**
   * The facet lookup takes the same route as the identifier: the catalog reported here is the one
   * that owns the table, not the cached catalog the write went through.
   */
  @Test
  void testGetOwningCatalogRecoversCatalogAndIdentifier() {
    TableCatalog owning = mock(TableCatalog.class);
    withSessionCatalog(context, "c", owning);

    Optional<RelationHandler.OwningCatalog> owner =
        handler.getOwningCatalog(relationOf(TABLE_LOCATION, QUALIFIED_NAME));

    assertThat(owner).isPresent();
    assertThat(owner.get().getCatalog()).isSameAs(owning);
    assertThat(owner.get().getIdentifier()).isEqualTo(Identifier.of(new String[] {"db"}, "tbl"));
  }

  /** A rewrite on Spark 4.1 resolves to the same owning catalog and identifier as on Spark 4.0. */
  @Test
  void testGetOwningCatalogForRewriteTable() {
    TableCatalog owning = mock(TableCatalog.class);
    withSessionCatalog(context, "c", owning);

    Optional<RelationHandler.OwningCatalog> owner =
        handler.getOwningCatalog(rewriteRelationOf(TABLE_LOCATION, QUALIFIED_NAME));

    assertThat(owner).isPresent();
    assertThat(owner.get().getCatalog()).isSameAs(owning);
    assertThat(owner.get().getIdentifier()).isEqualTo(Identifier.of(new String[] {"db"}, "tbl"));
  }

  @Test
  void testGetDatasetIdentifierForRewriteTableFallsBackToTableLocation() {
    when(context.getSparkSession()).thenReturn(Optional.empty());

    DatasetIdentifier di =
        handler.getDatasetIdentifier(rewriteRelationOf(TABLE_LOCATION, QUALIFIED_NAME));

    assertThat(di.getName()).isEqualTo(TABLE_PATH);
    assertThat(di.getNamespace()).isEqualTo("file");
  }

  /** A catalog registered under a name that is not a {@link TableCatalog} cannot be resolved. */
  @Test
  void testGetOwningCatalogRejectsNonTableCatalog() {
    withSessionCatalog(context, "c", mock(CatalogPlugin.class));

    assertThat(handler.getOwningCatalog(relationOf(TABLE_LOCATION, QUALIFIED_NAME))).isEmpty();
  }

  /** Registers {@code catalog} under {@code name} in the context's session catalog manager. */
  private static void withSessionCatalog(
      OpenLineageContext context, String name, CatalogPlugin catalog) {
    SparkSession session = mock(SparkSession.class, RETURNS_DEEP_STUBS);
    when(session.sessionState().catalogManager().catalog(name)).thenReturn(catalog);
    when(context.getSparkSession()).thenReturn(Optional.of(session));
  }

  private DataSourceV2Relation relationOf(String location, String name) {
    Table icebergTable = icebergTable(location, name);
    SparkTable sparkTable = mock(SparkTable.class);
    when(sparkTable.table()).thenReturn(icebergTable);
    return relationOver(sparkTable);
  }

  private DataSourceV2Relation rewriteRelationOf(String location, String name) {
    return relationOver(new SparkRewriteTable(icebergTable(location, name)));
  }

  private static Table icebergTable(String location, String name) {
    Table icebergTable = mock(Table.class);
    when(icebergTable.location()).thenReturn(location);
    when(icebergTable.name()).thenReturn(name);
    return icebergTable;
  }

  private static DataSourceV2Relation relationOver(
      org.apache.spark.sql.connector.catalog.Table table) {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
    when(relation.table()).thenReturn(table);
    return relation;
  }

  /** A Spark table outside Iceberg's rewrite path that still exposes a {@code table()} accessor. */
  public static class WrapperWithTableAccessor
      implements org.apache.spark.sql.connector.catalog.Table {
    private final Table icebergTable;

    WrapperWithTableAccessor(Table table) {
      this.icebergTable = table;
    }

    public Table table() {
      return icebergTable;
    }

    @Override
    public String name() {
      return "wrapper";
    }

    @Override
    public StructType schema() {
      return new StructType();
    }

    @Override
    public java.util.Set<org.apache.spark.sql.connector.catalog.TableCapability> capabilities() {
      return Collections.emptySet();
    }
  }

  /** Builds a context whose factory contributes exactly the given relation handlers. */
  private static OpenLineageContext contextContributing(RelationHandler... handlers) {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getDatasetBuilderFactory())
        .thenReturn(
            new DatasetBuilderFactory() {
              @Override
              public Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>>
                  getInputBuilders(OpenLineageContext ctx) {
                return Collections.emptyList();
              }

              @Override
              public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
                  getOutputBuilders(OpenLineageContext ctx) {
                return Collections.emptyList();
              }

              @Override
              public List<RelationHandler> getRelationHandlers(OpenLineageContext ctx) {
                return Arrays.asList(handlers);
              }
            });
    return context;
  }
}
