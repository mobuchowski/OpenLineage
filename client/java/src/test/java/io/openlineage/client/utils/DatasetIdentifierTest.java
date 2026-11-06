/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.dataset.partition.trimmer.DatasetNameTrimmer;
import io.openlineage.client.dataset.partition.trimmer.DateTrimmer;
import io.openlineage.client.dataset.partition.trimmer.KeyValueTrimmer;
import java.util.Arrays;
import java.util.Collection;
import org.junit.jupiter.api.Test;

class DatasetIdentifierTest {

  Collection<DatasetNameTrimmer> trimmers = Arrays.asList(new KeyValueTrimmer(), new DateTrimmer());

  @Test
  void testWithTrimmedName() {
    DatasetIdentifier di = new DatasetIdentifier("/tmp/", "namespace");
    assertThat(di.withTrimmedName(trimmers)).isEqualTo(di);

    di = new DatasetIdentifier("/tmp/key=value/2025-01-01/key=value", "namespace");
    assertThat(di.withTrimmedName(trimmers).getName()).isEqualTo("/tmp");
  }

  @Test
  void testCatalogTableName() {
    DatasetIdentifier.CatalogTableName catalogName =
        new DatasetIdentifier.CatalogTableName("db", "t", "delta");
    DatasetIdentifier di =
        new DatasetIdentifier("/tmp/t/key=value", "namespace").withCatalogTableName(catalogName);

    assertThat(di.withTrimmedName(trimmers).getCatalogTableName()).isEqualTo(catalogName);
    // not part of the dataset's identity
    assertThat(di).isEqualTo(new DatasetIdentifier("/tmp/t/key=value", "namespace"));
  }
}
