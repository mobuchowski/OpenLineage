/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.FabricLakehouseUtils;
import io.openlineage.client.utils.FabricWarehouseUtils;
import io.openlineage.client.utils.OneLakeLocation;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * Adds the {@code oneLake} facet to datasets stored in OneLake, Microsoft Fabric's storage.
 *
 * <p>The facet is built from the dataset's own name, namespace and symlinks, so it describes the
 * dataset however the job reached it, including from outside Fabric. When the job runs in Fabric,
 * the session's settings can add the workspace's name, but only for the workspace the session runs
 * in: a job can read and write items in other workspaces.
 */
public final class OneLakeFacetUtils {
  public static final String FACET_NAME = "oneLake";
  public static final String WORKSPACE_NAME_CONF = "trident.workspace.name";

  private OneLakeFacetUtils() {}

  /**
   * Builds the {@code oneLake} facet of a dataset.
   *
   * @param context the OpenLineage context, for the session's settings and the disabled facets
   * @param identifier the dataset, with its symlinks
   * @return the facet, or empty if the dataset isn't stored in OneLake or the facet is disabled
   */
  public static Optional<OpenLineage.OneLakeDatasetFacet> facetFor(
      OpenLineageContext context, DatasetIdentifier identifier) {
    return facetFor(context, identifier, Optional.empty(), Optional.empty());
  }

  /**
   * Builds the {@code oneLake} facet of a dataset.
   *
   * @param context the OpenLineage context, for the session's settings and the disabled facets
   * @param identifier the dataset, with its symlinks
   * @param known what is already known about the dataset's OneLake location, for example from
   *     {@link FabricLakehouseSparkUtils#resolve}; if empty, it is worked out from the identifier
   * @param tableId the table's Delta table ID, if known
   * @return the facet, or empty if the dataset isn't stored in OneLake or the facet is disabled
   */
  public static Optional<OpenLineage.OneLakeDatasetFacet> facetFor(
      OpenLineageContext context,
      DatasetIdentifier identifier,
      Optional<OneLakeLocation> known,
      Optional<String> tableId) {
    if (FacetUtils.isFacetDisabled(context, FACET_NAME)) {
      return Optional.empty();
    }
    Optional<OneLakeLocation> location = known;
    if (!location.isPresent()) {
      location = FabricWarehouseUtils.toOneLakeLocation(identifier);
    }
    if (!location.isPresent()) {
      location = FabricLakehouseUtils.toOneLakeLocation(identifier);
    }
    if (!location.isPresent()) {
      location = OneLakeLocation.parse(identifier.getNamespace(), identifier.getName());
    }
    Function<String, Optional<String>> conf =
        context
            .getSparkSession()
            .map(FabricWarehouseSparkUtils::confOf)
            .orElse(key -> Optional.empty());
    return location
        .map(l -> withWorkspaceName(l, conf))
        .map(l -> tableId.map(id -> l.toBuilder().tableId(id).build()).orElse(l))
        .map(l -> l.toFacet(context.getOpenLineage()));
  }

  /**
   * Adds the workspace's name from {@code trident.workspace.name}, if the location is in the
   * workspace the Fabric session runs in ({@code trident.workspace.id}).
   */
  static OneLakeLocation withWorkspaceName(
      OneLakeLocation location, Function<String, Optional<String>> conf) {
    if (location.getWorkspaceName() != null || location.getWorkspaceId() == null) {
      return location;
    }
    Optional<UUID> sessionWorkspace =
        conf.apply(FabricWarehouseSparkUtils.WORKSPACE_ID_CONF)
            .flatMap(FabricWarehouseUtils::parseGuid);
    if (!sessionWorkspace.filter(location.getWorkspaceId()::equals).isPresent()) {
      return location;
    }
    return conf.apply(WORKSPACE_NAME_CONF)
        .filter(name -> !name.trim().isEmpty())
        .map(name -> location.toBuilder().workspaceName(name).build())
        .orElse(location);
  }
}
