/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * How OpenLineage names tables in Microsoft Fabric lakehouses.
 *
 * <p>A lakehouse table is named {@code fabric-lakehouse://<workspace ID>} plus {@code
 * <lakehouse>.<schema>.<table>}, with the workspace ID in lowercase. Lakehouses created without
 * schemas keep their tables directly under {@code Tables/}; those tables use the schema {@code
 * dbo}, the name Fabric itself gives them in SQL.
 *
 * <p>The table's files are in OneLake at {@code abfss://<workspace
 * ID>@onelake.dfs.fabric.microsoft.com/<lakehouse ID>/Tables/[<schema>/]<table>}. When the
 * lakehouse's ID is known, that location is added as a {@code LOCATION} symlink.
 */
public final class FabricLakehouseUtils {
  public static final String SCHEME = "fabric-lakehouse";
  public static final String DEFAULT_SCHEMA = "dbo";

  private static final int TABLE_NAME_PARTS = 3;

  private FabricLakehouseUtils() {}

  /**
   * Names a lakehouse table from where it is in OneLake.
   *
   * @param location the table's location; it must have a workspace ID, a lakehouse name and a
   *     {@code Tables/[<schema>/]<table>} path
   * @return the table, with a {@code LOCATION} symlink when the lakehouse ID is known, or empty if
   *     the location doesn't say enough
   */
  public static Optional<DatasetIdentifier> fromOneLakeLocation(OneLakeLocation location) {
    Optional<List<String>> parts = location.tableParts();
    if (location.getWorkspaceId() == null || location.getItemName() == null || !parts.isPresent()) {
      return Optional.empty();
    }
    List<String> table = parts.get();
    String schema = table.size() == 1 ? DEFAULT_SCHEMA : table.get(0);
    DatasetIdentifier identifier =
        new DatasetIdentifier(
            location.getItemName() + "." + schema + "." + table.get(table.size() - 1),
            SCHEME + "://" + location.getWorkspaceId());
    location
        .guidIdentifier()
        .ifPresent(
            guid ->
                identifier.withSymlink(
                    guid.getName(), guid.getNamespace(), DatasetIdentifier.SymlinkType.LOCATION));
    return Optional.of(identifier);
  }

  /**
   * Describes a lakehouse table in OneLake terms, for the {@code fabric_onelake} facet: the
   * workspace ID from the namespace, the lakehouse from the name, and the lakehouse ID and location
   * from the {@code LOCATION} symlink, if it is in the same workspace.
   *
   * @return the description, or empty if this isn't a {@code fabric-lakehouse} dataset named {@code
   *     <lakehouse>.<schema>.<table>}
   */
  public static Optional<OneLakeLocation> toOneLakeLocation(DatasetIdentifier identifier) {
    String prefix = SCHEME + "://";
    String[] parts = identifier.getName().split("\\.");
    if (!identifier.getNamespace().startsWith(prefix) || parts.length != TABLE_NAME_PARTS) {
      return Optional.empty();
    }
    Optional<UUID> workspaceId =
        OneLakeLocation.guid(identifier.getNamespace().substring(prefix.length()));
    if (!workspaceId.isPresent()) {
      return Optional.empty();
    }
    OneLakeLocation.OneLakeLocationBuilder builder =
        OneLakeLocation.builder()
            .workspaceId(workspaceId.get())
            .itemName(parts[0])
            .itemType(OneLakeLocation.LAKEHOUSE)
            .path(Arrays.asList(OneLakeLocation.TABLES, parts[1], parts[2]));
    if (identifier.getSymlinks() != null) {
      identifier.getSymlinks().stream()
          .filter(s -> s.getType() == DatasetIdentifier.SymlinkType.LOCATION)
          .map(s -> OneLakeLocation.parse(s.getNamespace(), s.getName()))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .filter(location -> workspaceId.get().equals(location.getWorkspaceId()))
          .filter(location -> location.getItemId() != null)
          .findFirst()
          .ifPresent(location -> builder.itemId(location.getItemId()).path(location.getPath()));
    }
    return Optional.of(builder.build());
  }
}
