/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.ir;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Intermediate representation of a single LogicalPlan node.
 *
 * <p>Captures the node type (Spark class simple name), output columns, child references,
 * and node-specific attributes extracted from the serialized plan.
 *
 * <p>The IR intentionally does not reproduce full expression trees — expressions are simplified
 * or omitted. What matters for OL performance testing is the plan structure (node types, tree
 * shape, schema widths) and leaf node configuration (relation type, file counts).
 */
public class NodeIR {
  private int id;
  private String type;
  private List<Integer> children = new ArrayList<>();
  private List<ColumnIR> output = new ArrayList<>();
  private int truncatedColumns;
  private Map<String, String> attributes = new HashMap<>();
  private List<ColumnIR> conditionColumns = new ArrayList<>();

  /** The full class name from Spark JSON format, if available. */
  private String className;

  public NodeIR() {}

  public NodeIR(int id, String type) {
    this.id = id;
    this.type = type;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public List<Integer> getChildren() {
    return children;
  }

  public void setChildren(List<Integer> children) {
    this.children = children;
  }

  public List<ColumnIR> getOutput() {
    return output;
  }

  public void setOutput(List<ColumnIR> output) {
    this.output = output;
  }

  public int getTruncatedColumns() {
    return truncatedColumns;
  }

  public void setTruncatedColumns(int truncatedColumns) {
    this.truncatedColumns = truncatedColumns;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public List<ColumnIR> getConditionColumns() {
    return conditionColumns;
  }

  public void setConditionColumns(List<ColumnIR> conditionColumns) {
    this.conditionColumns = conditionColumns;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  @Override
  public String toString() {
    return "NodeIR{id=" + id + ", type=" + type + ", children=" + children
        + ", output=" + output.size() + " cols"
        + (truncatedColumns > 0 ? " + " + truncatedColumns + " truncated" : "")
        + "}";
  }
}
