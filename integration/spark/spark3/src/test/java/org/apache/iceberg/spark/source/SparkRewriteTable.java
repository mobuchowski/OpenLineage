/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.iceberg.spark.source;

import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;

/**
 * Stands in for Iceberg 1.11's {@code SparkRewriteTable}, which the Iceberg version these tests
 * compile against does not have. Only the fully qualified name and the public {@code table()}
 * accessor matter to the code under test.
 */
public class SparkRewriteTable implements org.apache.spark.sql.connector.catalog.Table {
  private final Table icebergTable;

  public SparkRewriteTable(Table table) {
    this.icebergTable = table;
  }

  public Table table() {
    return icebergTable;
  }

  @Override
  public String name() {
    return icebergTable.name();
  }

  @Override
  public StructType schema() {
    return new StructType();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.emptySet();
  }
}
