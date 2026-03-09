/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.parser;

import java.util.List;

public abstract class ExprNode {

  public static class AliasExpr extends ExprNode {
    private final ExprNode child;
    private final String name;
    private final long exprId;
    private final String typeSuffix;

    public AliasExpr(ExprNode child, String name, long exprId, String typeSuffix) {
      this.child = child;
      this.name = name;
      this.exprId = exprId;
      this.typeSuffix = typeSuffix;
    }

    public ExprNode getChild() {
      return child;
    }

    public String getName() {
      return name;
    }

    public long getExprId() {
      return exprId;
    }

    public String getTypeSuffix() {
      return typeSuffix;
    }

    @Override
    public String toString() {
      return child + " AS " + name + "#" + exprId + typeSuffix;
    }
  }

  public static class AttributeRef extends ExprNode {
    private final String name;
    private final long exprId;
    private final boolean isLong;

    public AttributeRef(String name, long exprId, boolean isLong) {
      this.name = name;
      this.exprId = exprId;
      this.isLong = isLong;
    }

    public String getName() {
      return name;
    }

    public long getExprId() {
      return exprId;
    }

    public boolean isLong() {
      return isLong;
    }

    @Override
    public String toString() {
      return name + "#" + exprId + (isLong ? "L" : "");
    }
  }

  public static class FunctionCall extends ExprNode {
    private final String name;
    private final List<ExprNode> args;

    public FunctionCall(String name, List<ExprNode> args) {
      this.name = name;
      this.args = args;
    }

    public String getName() {
      return name;
    }

    public List<ExprNode> getArgs() {
      return args;
    }

    @Override
    public String toString() {
      return name + "(" + args + ")";
    }
  }

  public static class CastExpr extends ExprNode {
    private final ExprNode child;
    private final DataTypeNode targetType;

    public CastExpr(ExprNode child, DataTypeNode targetType) {
      this.child = child;
      this.targetType = targetType;
    }

    public ExprNode getChild() {
      return child;
    }

    public DataTypeNode getTargetType() {
      return targetType;
    }

    @Override
    public String toString() {
      return "cast(" + child + " as " + targetType + ")";
    }
  }

  public static class CaseWhenExpr extends ExprNode {
    private final List<ExprNode[]> branches;
    private final ExprNode elseValue;

    public CaseWhenExpr(List<ExprNode[]> branches, ExprNode elseValue) {
      this.branches = branches;
      this.elseValue = elseValue;
    }

    public List<ExprNode[]> getBranches() {
      return branches;
    }

    public ExprNode getElseValue() {
      return elseValue;
    }

    @Override
    public String toString() {
      return "CASE(" + branches.size() + " branches)";
    }
  }

  public static class IfExpr extends ExprNode {
    private final ExprNode condition;
    private final ExprNode trueValue;
    private final ExprNode falseValue;

    public IfExpr(ExprNode condition, ExprNode trueValue, ExprNode falseValue) {
      this.condition = condition;
      this.trueValue = trueValue;
      this.falseValue = falseValue;
    }

    public ExprNode getCondition() {
      return condition;
    }

    public ExprNode getTrueValue() {
      return trueValue;
    }

    public ExprNode getFalseValue() {
      return falseValue;
    }

    @Override
    public String toString() {
      return "if(" + condition + ", " + trueValue + ", " + falseValue + ")";
    }
  }

  public static class InputRef extends ExprNode {
    private final int index;
    private final String className;
    private final boolean nullable;

    public InputRef(int index, String className, boolean nullable) {
      this.index = index;
      this.className = className;
      this.nullable = nullable;
    }

    public int getIndex() {
      return index;
    }

    public String getClassName() {
      return className;
    }

    public boolean isNullable() {
      return nullable;
    }

    @Override
    public String toString() {
      return "input[" + index + ", " + className + ", " + nullable + "]";
    }
  }

  public static class LiteralExpr extends ExprNode {
    private final String value;
    private final String dataType;

    public LiteralExpr(String value, String dataType) {
      this.value = value;
      this.dataType = dataType;
    }

    public String getValue() {
      return value;
    }

    public String getDataType() {
      return dataType;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public static class BinaryExpr extends ExprNode {
    private final ExprNode left;
    private final String operator;
    private final ExprNode right;

    public BinaryExpr(ExprNode left, String operator, ExprNode right) {
      this.left = left;
      this.operator = operator;
      this.right = right;
    }

    public ExprNode getLeft() {
      return left;
    }

    public String getOperator() {
      return operator;
    }

    public ExprNode getRight() {
      return right;
    }

    @Override
    public String toString() {
      return "(" + left + " " + operator + " " + right + ")";
    }
  }

  public static class UnaryExpr extends ExprNode {
    private final String operator;
    private final ExprNode child;

    public UnaryExpr(String operator, ExprNode child) {
      this.operator = operator;
      this.child = child;
    }

    public String getOperator() {
      return operator;
    }

    public ExprNode getChild() {
      return child;
    }

    @Override
    public String toString() {
      return operator + "(" + child + ")";
    }
  }

  public static class FieldAccess extends ExprNode {
    private final ExprNode object;
    private final String fieldName;

    public FieldAccess(ExprNode object, String fieldName) {
      this.object = object;
      this.fieldName = fieldName;
    }

    public ExprNode getObject() {
      return object;
    }

    public String getFieldName() {
      return fieldName;
    }

    @Override
    public String toString() {
      return object + "." + fieldName;
    }
  }

  public static class InExpr extends ExprNode {
    private final ExprNode value;
    private final List<ExprNode> list;

    public InExpr(ExprNode value, List<ExprNode> list) {
      this.value = value;
      this.list = list;
    }

    public ExprNode getValue() {
      return value;
    }

    public List<ExprNode> getList() {
      return list;
    }

    @Override
    public String toString() {
      return value + " IN (" + list + ")";
    }
  }

  public static class OpaqueExpr extends ExprNode {
    private final String rawText;

    public OpaqueExpr(String rawText) {
      this.rawText = rawText;
    }

    public String getRawText() {
      return rawText;
    }

    @Override
    public String toString() {
      return "OPAQUE(" + rawText + ")";
    }
  }

  public static class TruncationMarker extends ExprNode {
    private final int count;

    public TruncationMarker(int count) {
      this.count = count;
    }

    public int getCount() {
      return count;
    }

    @Override
    public String toString() {
      return "... " + count + " more fields";
    }
  }
}
