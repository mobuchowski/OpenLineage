/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.client.utils.OneLakeLocation;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// The IDs, names and encoded catalog names below come from a real Fabric test workspace
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class FabricLakehouseSparkUtilsTest {
  private static final String WS = "d1f6473b-9f7a-4352-940b-a8d74461d3fc";
  private static final String OTHER_WS = "45ff911e-5e75-428a-a618-52958fcc7604";
  private static final String LH = "c7a14054-60a7-4121-8fb7-252d16f687eb";
  private static final String LH_LEGACY = "d8a75818-4429-41c0-bc80-81da494ec02f";
  private static final String WH = "d75873bd-716b-4378-a37e-864c76fc9b3b";
  private static final String ONELAKE = "abfss://" + WS + "@onelake.dfs.fabric.microsoft.com";
  private static final String LAKEHOUSE_NS = "fabric-lakehouse://" + WS;
  // base32hex of default%do-test-env-fabric%lh_shopist%probe551
  private static final String SALES_DB =
      "chimcobldhq2ap3f5lq6asrk5limsthdcpgm4sj9ccimoq2vedk6us39edq2as3idth6ad9l64";
  // base32hex of default%do-test-env-fabric%lh_shopist_legacy%dbo
  private static final String LEGACY_DB =
      "chimcobldhq2ap3f5lq6asrk5limsthdcpgm4sj9ccimoq2vedk6us39edq5ur35ctgm6u95chh6u";

  private final Map<String, String> conf = new HashMap<>();
  private final Function<String, Optional<String>> lookup =
      key -> Optional.ofNullable(conf.get(key));

  @BeforeEach
  void fabricSession() {
    // the settings of a notebook whose default lakehouse is lh_shopist, which has schemas
    conf.put("trident.workspace.id", WS);
    conf.put("trident.workspace.name", "do-test-env-fabric");
    conf.put("trident.lakehouse.id", LH);
    conf.put("trident.lakehouse.name", "lh_shopist");
    conf.put("trident.catalog.metastore.workspaceId", WS);
    conf.put("trident.schema.name", "dbo");
  }

  @Test
  void testDecodeCatalogDatabase() {
    assertThat(FabricLakehouseSparkUtils.decodeBase32Hex(SALES_DB))
        .contains("default%do-test-env-fabric%lh_shopist%probe551");
    assertThat(FabricLakehouseSparkUtils.decodeBase32Hex(LEGACY_DB))
        .contains("default%do-test-env-fabric%lh_shopist_legacy%dbo");
    // not base32hex, or leftover bits that aren't zero
    assertThat(FabricLakehouseSparkUtils.decodeBase32Hex("lh_shopist_legacy")).isEmpty();
    assertThat(FabricLakehouseSparkUtils.decodeBase32Hex("cv")).isEmpty();
  }

  @Test
  void testDefaultLakehouseById() {
    // rule (b): a read of the default lakehouse, by its OneLake path
    FabricLakehouseSparkUtils.Resolved resolved =
        resolve(new DatasetIdentifier(LH + "/Tables/sales/orders", ONELAKE), false, false);

    assertThat(resolved.getIdentifier().getNamespace()).isEqualTo(LAKEHOUSE_NS);
    assertThat(resolved.getIdentifier().getName()).isEqualTo("lh_shopist.sales.orders");
    assertThat(resolved.getIdentifier().getSymlinks())
        .containsExactly(new Symlink(LH + "/Tables/sales/orders", ONELAKE, SymlinkType.LOCATION));
    OneLakeLocation location = resolved.getOneLake().get();
    assertThat(location.getItemType()).isEqualTo("Lakehouse");
    assertThat(location.getItemName()).isEqualTo("lh_shopist");
  }

  @Test
  void testDefaultLakehouseWithoutSchemas() {
    conf.put("trident.lakehouse.id", LH_LEGACY);
    conf.put("trident.lakehouse.name", "lh_shopist_legacy");
    conf.remove("trident.schema.name");

    DatasetIdentifier identifier =
        resolve(new DatasetIdentifier(LH_LEGACY + "/Tables/orders_daily", ONELAKE), true, true)
            .getIdentifier();

    assertThat(identifier.getNamespace()).isEqualTo(LAKEHOUSE_NS);
    assertThat(identifier.getName()).isEqualTo("lh_shopist_legacy.dbo.orders_daily");
    assertThat(identifier.getSymlinks())
        .containsExactly(
            new Symlink(LH_LEGACY + "/Tables/orders_daily", ONELAKE, SymlinkType.LOCATION));
  }

  @Test
  void testDefaultLakehouseInAnotherWorkspaceKeepsThePathsWorkspace() {
    // the default lakehouse's ID is matched; the workspace ID always comes from the path
    conf.put("trident.workspace.id", OTHER_WS);

    DatasetIdentifier identifier =
        resolve(new DatasetIdentifier(LH + "/Tables/sales/orders", ONELAKE), false, false)
            .getIdentifier();

    assertThat(identifier.getNamespace()).isEqualTo(LAKEHOUSE_NS);
  }

  @Test
  void testCatalogWriteToAnotherLakehouse() {
    // rules (a) and (c): saveAsTable("lh_shopist_legacy.t") from a lh_shopist notebook
    DatasetIdentifier written =
        withCatalog(new DatasetIdentifier(LH_LEGACY + "/Tables/t", ONELAKE), LEGACY_DB, "t");

    FabricLakehouseSparkUtils.Resolved resolved = resolve(written, true, false);

    assertThat(resolved.getIdentifier().getNamespace()).isEqualTo(LAKEHOUSE_NS);
    assertThat(resolved.getIdentifier().getName()).isEqualTo("lh_shopist_legacy.dbo.t");
    assertThat(resolved.getIdentifier().getSymlinks())
        .containsExactly(new Symlink(LH_LEGACY + "/Tables/t", ONELAKE, SymlinkType.LOCATION));
    assertThat(resolved.getOneLake().get().getWorkspaceName()).isEqualTo("do-test-env-fabric");
  }

  @Test
  void testCatalogReadOfAnotherItemIsNotNamedAsLakehouse() {
    // rule (c) alone: the catalog lists warehouses too, so a read only names the item. The
    // database is base32hex of default%do-test-env-fabric%wh_primary%raw.
    DatasetIdentifier read =
        withCatalog(
            new DatasetIdentifier(WH + "/Tables/raw/orders", ONELAKE),
            "chimcobldhq2ap3f5lq6asrk5limsthdcpgm4sj9ccineq2ve1p6irb1e9siasj1es",
            "orders");

    FabricLakehouseSparkUtils.Resolved resolved = resolve(read, false, false);

    assertThat(resolved.getIdentifier())
        .isEqualTo(new DatasetIdentifier(WH + "/Tables/raw/orders", ONELAKE));
    OneLakeLocation location = resolved.getOneLake().get();
    assertThat(location.getItemName()).isEqualTo("wh_primary");
    assertThat(location.getItemType()).isNull();
    assertThat(location.getItemId().toString()).isEqualTo(WH);
  }

  @Test
  void testDeltaPathWriteToAnotherLakehouse() {
    // rule (a) without a name: the item is a lakehouse, but its name isn't known
    FabricLakehouseSparkUtils.Resolved resolved =
        resolve(new DatasetIdentifier(LH_LEGACY + "/Tables/t", ONELAKE), true, true);

    assertThat(resolved.getIdentifier())
        .isEqualTo(new DatasetIdentifier(LH_LEGACY + "/Tables/t", ONELAKE));
    assertThat(resolved.getOneLake().get().getItemType()).isEqualTo("Lakehouse");
    assertThat(resolved.getOneLake().get().getItemName()).isNull();
  }

  @Test
  void testRuleAOnlyForDeltaWritesUnderTables() {
    DatasetIdentifier path = new DatasetIdentifier(LH_LEGACY + "/Tables/t", ONELAKE);
    assertThat(resolve(path, false, true).getOneLake().get().getItemType()).isNull();
    assertThat(resolve(path, true, false).getOneLake().get().getItemType()).isNull();
    assertThat(
            resolve(new DatasetIdentifier(LH_LEGACY + "/Files/t", ONELAKE), true, true)
                .getOneLake()
                .get()
                .getItemType())
        .isNull();
  }

  @Test
  void testNameFormLakehousePathInTheSessionsWorkspace() {
    // rule (d): the workspace name is the session's, so its ID is known
    FabricLakehouseSparkUtils.Resolved resolved =
        resolve(
            new DatasetIdentifier(
                "lh_shopist_legacy.Lakehouse/Tables/orders_daily",
                "abfss://do-test-env-fabric@onelake.dfs.fabric.microsoft.com"),
            false,
            false);

    assertThat(resolved.getIdentifier().getNamespace()).isEqualTo(LAKEHOUSE_NS);
    assertThat(resolved.getIdentifier().getName()).isEqualTo("lh_shopist_legacy.dbo.orders_daily");
    // the lakehouse's ID isn't known, so there is no location
    assertThat(resolved.getIdentifier().getSymlinks()).isEmpty();
  }

  @Test
  void testNameFormPathOfTheDefaultLakehouse() {
    DatasetIdentifier identifier =
        resolve(
                new DatasetIdentifier(
                    "lh_shopist.Lakehouse/Tables/sales/orders",
                    "abfss://do-test-env-fabric@onelake.dfs.fabric.microsoft.com"),
                false,
                false)
            .getIdentifier();

    assertThat(identifier.getName()).isEqualTo("lh_shopist.sales.orders");
    assertThat(identifier.getSymlinks())
        .containsExactly(new Symlink(LH + "/Tables/sales/orders", ONELAKE, SymlinkType.LOCATION));
  }

  @Test
  void testNameFormPathInAnotherWorkspace() {
    FabricLakehouseSparkUtils.Resolved resolved =
        resolve(
            new DatasetIdentifier(
                "lh_x.Lakehouse/Tables/t", "abfss://other-ws@onelake.blob.fabric.microsoft.com"),
            false,
            false);

    // written the standard way, but still by name
    assertThat(resolved.getIdentifier())
        .isEqualTo(
            new DatasetIdentifier(
                "lh_x.Lakehouse/Tables/t", "abfss://other-ws@onelake.dfs.fabric.microsoft.com"));
    assertThat(resolved.getOneLake().get().getItemType()).isEqualTo("Lakehouse");
  }

  @Test
  void testNameFormWarehousePath() {
    FabricLakehouseSparkUtils.Resolved resolved =
        resolve(
            new DatasetIdentifier(
                "wh_primary.Datawarehouse/Tables/raw/orders",
                "abfss://do-test-env-fabric@onelake.dfs.fabric.microsoft.com"),
            false,
            false);

    assertThat(resolved.getIdentifier())
        .isEqualTo(
            new DatasetIdentifier(
                "wh_primary.Warehouse/Tables/raw/orders",
                "abfss://do-test-env-fabric@onelake.dfs.fabric.microsoft.com"));
    OneLakeLocation location = resolved.getOneLake().get();
    assertThat(location.getItemType()).isEqualTo("Warehouse");
    assertThat(location.getWorkspaceId().toString()).isEqualTo(WS);
  }

  @Test
  void testOutsideFabric() {
    conf.clear();

    FabricLakehouseSparkUtils.Resolved resolved =
        resolve(
            new DatasetIdentifier(LH.toUpperCase() + "/Tables/sales/orders/", ONELAKE), true, true);

    // the path is written the standard way, and the write shows that the item is a lakehouse
    assertThat(resolved.getIdentifier())
        .isEqualTo(new DatasetIdentifier(LH + "/Tables/sales/orders", ONELAKE));
    OneLakeLocation location = resolved.getOneLake().get();
    assertThat(location.getItemType()).isEqualTo("Lakehouse");
    assertThat(location.getItemName()).isNull();
    assertThat(location.guidLocation()).contains(ONELAKE + "/" + LH + "/Tables/sales/orders");
  }

  @Test
  void testCatalogNameIgnoredOutsideFabric() {
    conf.clear();
    DatasetIdentifier identifier = new DatasetIdentifier("/warehouse/db.db/t", "file");

    assertThat(withCatalog(identifier, "db", "t").getSymlinks()).isEmpty();
  }

  @Test
  void testCatalogNameInSessionWithoutSchemas() {
    conf.put("trident.lakehouse.id", LH_LEGACY);
    conf.put("trident.lakehouse.name", "lh_shopist_legacy");
    conf.remove("trident.schema.name");
    // another lakehouse in the same workspace, as a plain catalog database
    DatasetIdentifier read =
        withCatalog(
            new DatasetIdentifier(LH + "/Tables/orders", ONELAKE), "tmp_other_lh", "orders");

    FabricLakehouseSparkUtils.Resolved resolved = resolve(read, false, false);

    assertThat(resolved.getIdentifier())
        .isEqualTo(new DatasetIdentifier(LH + "/Tables/orders", ONELAKE));
    assertThat(resolved.getOneLake().get().getItemName()).isEqualTo("tmp_other_lh");
  }

  @Test
  void testUndecodableCatalogNameInSessionWithSchemas() {
    DatasetIdentifier read =
        withCatalog(new DatasetIdentifier(LH_LEGACY + "/Tables/t", ONELAKE), "not%base32", "t");

    assertThat(read.getSymlinks()).isEmpty();
  }

  @Test
  void testOtherDatasetsAreUnchanged() {
    DatasetIdentifier s3 = new DatasetIdentifier("bucket/path", "s3://bucket");
    FabricLakehouseSparkUtils.Resolved resolved = resolve(s3, true, true);

    assertThat(resolved.getIdentifier()).isSameAs(s3);
    assertThat(resolved.getOneLake()).isEmpty();
  }

  @Test
  void testInternalCatalogSymlinkIsAlwaysRemoved() {
    DatasetIdentifier identifier = new DatasetIdentifier("/data/t", "file");
    identifier.withSymlink("db.t", "hive://metastore", SymlinkType.TABLE);
    identifier.withSymlink(
        "a/b/c/d/delta", FabricLakehouseSparkUtils.CATALOG_HINT_NAMESPACE, SymlinkType.TABLE);

    assertThat(resolve(identifier, false, false).getIdentifier().getSymlinks())
        .containsExactly(new Symlink("db.t", "hive://metastore", SymlinkType.TABLE));
  }

  @Test
  void testResolveInputMatchesInputDatasets() {
    DatasetIdentifier identifier = new DatasetIdentifier(LH + "/Tables/sales/orders", ONELAKE);

    assertThat(FabricLakehouseSparkUtils.resolveInput(identifier, lookup))
        .isEqualTo(resolve(identifier, false, false).getIdentifier());
  }

  private FabricLakehouseSparkUtils.Resolved resolve(
      DatasetIdentifier identifier, boolean output, boolean delta) {
    return FabricLakehouseSparkUtils.resolve(identifier, output, delta, lookup);
  }

  private DatasetIdentifier withCatalog(
      DatasetIdentifier identifier, String database, String table) {
    return FabricLakehouseSparkUtils.withCatalogTable(
        identifier, lookup, Optional.of(database), table, true);
  }
}
