/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.parser;

import java.util.ArrayList;
import java.util.List;

public class DataTypeNode {
  private final String typeName;
  private final List<DataTypeNode> typeArgs;
  private final List<FieldDef> fields;

  public DataTypeNode(String typeName) {
    this(typeName, new ArrayList<DataTypeNode>(), new ArrayList<FieldDef>());
  }

  public DataTypeNode(String typeName, List<DataTypeNode> typeArgs, List<FieldDef> fields) {
    this.typeName = typeName;
    this.typeArgs = typeArgs;
    this.fields = fields;
  }

  public String getTypeName() {
    return typeName;
  }

  public List<DataTypeNode> getTypeArgs() {
    return typeArgs;
  }

  public List<FieldDef> getFields() {
    return fields;
  }

  @Override
  public String toString() {
    if (!fields.isEmpty()) {
      StringBuilder sb = new StringBuilder(typeName).append("<");
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append(fields.get(i));
      }
      sb.append(">");
      return sb.toString();
    }
    if (!typeArgs.isEmpty()) {
      StringBuilder sb = new StringBuilder(typeName).append("<");
      for (int i = 0; i < typeArgs.size(); i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append(typeArgs.get(i));
      }
      sb.append(">");
      return sb.toString();
    }
    return typeName;
  }

  public static class FieldDef {
    private final String name;
    private final DataTypeNode type;

    public FieldDef(String name, DataTypeNode type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public DataTypeNode getType() {
      return type;
    }

    @Override
    public String toString() {
      return name + ":" + type;
    }
  }
}
