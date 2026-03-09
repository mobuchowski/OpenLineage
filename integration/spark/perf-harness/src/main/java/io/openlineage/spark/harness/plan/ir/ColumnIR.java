/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.ir;

/**
 * A column reference extracted from a serialized plan. Captures name, Spark ExprId, and data type.
 *
 * <p>ExprIds are critical for column-level lineage (CLL) — they connect output columns to input
 * columns across plan nodes. The parser extracts them from the {@code name#id} format in Spark's
 * treeString output or from the JSON representation.
 */
public class ColumnIR {
  private String name;
  private long exprId;
  private String dataType;

  public ColumnIR() {}

  public ColumnIR(String name, long exprId, String dataType) {
    this.name = name;
    this.exprId = exprId;
    this.dataType = dataType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getExprId() {
    return exprId;
  }

  public void setExprId(long exprId) {
    this.exprId = exprId;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  @Override
  public String toString() {
    return name + "#" + exprId + ":" + dataType;
  }
}
