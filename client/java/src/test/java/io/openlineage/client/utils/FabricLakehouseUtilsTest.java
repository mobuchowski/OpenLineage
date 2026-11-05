/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

// The IDs and names below are those of a real Fabric test workspace
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class FabricLakehouseUtilsTest {
  private static final String WS = "d1f6473b-9f7a-4352-940b-a8d74461d3fc";
  private static final String LH = "c7a14054-60a7-4121-8fb7-252d16f687eb";
  private static final String ONELAKE = "abfss://" + WS + "@onelake.dfs.fabric.microsoft.com";

  @Test
  void testFromOneLakeLocationWithSchema() {
    OneLakeLocation location =
        OneLakeLocation.parse(ONELAKE + "/" + LH + "/Tables/sales/orders").get().toBuilder()
            .itemName("lh_shopist")
            .itemType(OneLakeLocation.LAKEHOUSE)
            .build();

    DatasetIdentifier identifier = FabricLakehouseUtils.fromOneLakeLocation(location).get();

    assertThat(identifier.getNamespace()).isEqualTo("fabric-lakehouse://" + WS);
    assertThat(identifier.getName()).isEqualTo("lh_shopist.sales.orders");
    assertThat(identifier.getSymlinks())
        .containsExactly(
            new DatasetIdentifier.Symlink(
                LH + "/Tables/sales/orders", ONELAKE, DatasetIdentifier.SymlinkType.LOCATION));
  }

  @Test
  void testFromOneLakeLocationWithoutSchemas() {
    OneLakeLocation location =
        OneLakeLocation.builder()
            .workspaceId(UUID.fromString(WS))
            .itemName("lh_shopist_legacy")
            .itemType(OneLakeLocation.LAKEHOUSE)
            .path(Arrays.asList("Tables", "orders_daily"))
            .build();

    DatasetIdentifier identifier = FabricLakehouseUtils.fromOneLakeLocation(location).get();

    // no lakehouse ID, so no location
    assertThat(identifier.getName()).isEqualTo("lh_shopist_legacy.dbo.orders_daily");
    assertThat(identifier.getSymlinks()).isEmpty();
  }

  @Test
  void testFromOneLakeLocationNeedsWorkspaceIdNameAndTable() {
    OneLakeLocation location =
        OneLakeLocation.parse(ONELAKE + "/" + LH + "/Tables/sales/orders").get();

    assertThat(FabricLakehouseUtils.fromOneLakeLocation(location)).isEmpty();
    assertThat(
            FabricLakehouseUtils.fromOneLakeLocation(
                location.toBuilder().itemName("lh").path(Arrays.asList("Files", "a.csv")).build()))
        .isEmpty();
    assertThat(
            FabricLakehouseUtils.fromOneLakeLocation(
                location.toBuilder().itemName("lh").workspaceId(null).build()))
        .isEmpty();
  }

  @Test
  void testToOneLakeLocation() {
    DatasetIdentifier identifier =
        new DatasetIdentifier("lh_shopist.sales.orders", "fabric-lakehouse://" + WS)
            .withSymlink(
                LH + "/Tables/sales/orders", ONELAKE, DatasetIdentifier.SymlinkType.LOCATION);

    OneLakeLocation location = FabricLakehouseUtils.toOneLakeLocation(identifier).get();

    assertThat(location.getItemType()).isEqualTo("Lakehouse");
    assertThat(location.getItemName()).isEqualTo("lh_shopist");
    assertThat(location.getItemId()).isEqualTo(UUID.fromString(LH));
    assertThat(location.guidLocation()).contains(ONELAKE + "/" + LH + "/Tables/sales/orders");
  }

  @Test
  void testToOneLakeLocationOnlyForLakehouseTables() {
    assertThat(
            FabricLakehouseUtils.toOneLakeLocation(
                new DatasetIdentifier("lh.dbo.t", "fabric-lakehouse://not-a-guid")))
        .isEmpty();
    assertThat(
            FabricLakehouseUtils.toOneLakeLocation(
                new DatasetIdentifier("lh.t", "fabric-lakehouse://" + WS)))
        .isEmpty();
    assertThat(
            FabricLakehouseUtils.toOneLakeLocation(
                new DatasetIdentifier("lh.dbo.t", "fabric-warehouse://" + WS)))
        .isEmpty();
  }
}
