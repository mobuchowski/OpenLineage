/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.OneLakeLocation;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Adds the facets that follow from a dataset's identity: its final name, namespace and symlinks.
 */
public final class DatasetIdentityFacetUtils {
  private DatasetIdentityFacetUtils() {}

  /**
   * Adds every facet derived from the dataset's identity, such as {@code fabric_onelake} for
   * datasets stored in Microsoft Fabric's OneLake.
   */
  public static void buildIdentityFacets(
      OpenLineageContext context,
      DatasetCompositeFacetsBuilder facetsBuilder,
      DatasetIdentifier identifier) {
    buildIdentityFacets(context, facetsBuilder, identifier, Optional.empty(), Optional::empty);
  }

  /**
   * Adds every facet derived from the dataset's identity.
   *
   * @param oneLake what Fabric lakehouse naming already found out about the dataset in OneLake
   * @param tableIdLookup looks up the table's Delta table ID; only called for datasets in OneLake
   */
  public static void buildIdentityFacets(
      OpenLineageContext context,
      DatasetCompositeFacetsBuilder facetsBuilder,
      DatasetIdentifier identifier,
      Optional<OneLakeLocation> oneLake,
      Supplier<Optional<String>> tableIdLookup) {
    OneLakeFacetUtils.facetFor(
            context,
            identifier,
            oneLake,
            oneLake.isPresent() ? tableIdLookup.get() : Optional.empty())
        .ifPresent(facet -> facetsBuilder.getFacets().fabric_onelake(facet));
  }
}
