/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.parser;

import io.openlineage.spark.harness.plan.ir.ColumnIR;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Extracts {@link ColumnIR} instances from parsed expression lists.
 *
 * <p>Walks the AST to find {@link ExprNode.AliasExpr} nodes (output columns with names and
 * expr IDs) and {@link ExprNode.AttributeRef} nodes (direct column references in Relation nodes).
 * Also collects truncation counts from {@link ExprNode.TruncationMarker} nodes.
 */
public class ColumnExtractor {

  private ColumnExtractor() {}

  /**
   * Extract top-level output columns from an expression list.
   * Only looks at direct AliasExpr and AttributeRef nodes (not nested).
   */
  public static Result extract(List<ExprNode> expressions) {
    List<ColumnIR> columns = new ArrayList<>();
    int truncatedCount = 0;

    for (ExprNode expr : expressions) {
      if (expr instanceof ExprNode.AliasExpr) {
        ExprNode.AliasExpr alias = (ExprNode.AliasExpr) expr;
        String dataType = "L".equals(alias.getTypeSuffix()) ? "long" : "string";
        columns.add(new ColumnIR(alias.getName(), alias.getExprId(), dataType));
      } else if (expr instanceof ExprNode.AttributeRef) {
        ExprNode.AttributeRef ref = (ExprNode.AttributeRef) expr;
        String dataType = ref.isLong() ? "long" : "string";
        columns.add(new ColumnIR(ref.getName(), ref.getExprId(), dataType));
      } else if (expr instanceof ExprNode.TruncationMarker) {
        truncatedCount += ((ExprNode.TruncationMarker) expr).getCount();
      }
    }

    return new Result(columns, truncatedCount);
  }

  /**
   * Extract all column references from an expression tree by walking the full AST.
   * Used for conditions, filters, and other expressions where column refs are nested
   * inside binary operators, field accesses, function calls, etc.
   * Deduplicates by exprId.
   */
  public static List<ColumnIR> extractAllRefs(List<ExprNode> expressions) {
    List<ColumnIR> columns = new ArrayList<>();
    Set<Long> seen = new HashSet<>();
    for (ExprNode expr : expressions) {
      collectRefs(expr, columns, seen);
    }
    return columns;
  }

  private static void collectRefs(ExprNode expr, List<ColumnIR> columns, Set<Long> seen) {
    if (expr == null) {
      return;
    }
    if (expr instanceof ExprNode.AttributeRef) {
      ExprNode.AttributeRef ref = (ExprNode.AttributeRef) expr;
      if (seen.add(ref.getExprId())) {
        String dataType = ref.isLong() ? "long" : "string";
        columns.add(new ColumnIR(ref.getName(), ref.getExprId(), dataType));
      }
    } else if (expr instanceof ExprNode.AliasExpr) {
      collectRefs(((ExprNode.AliasExpr) expr).getChild(), columns, seen);
    } else if (expr instanceof ExprNode.BinaryExpr) {
      ExprNode.BinaryExpr bin = (ExprNode.BinaryExpr) expr;
      collectRefs(bin.getLeft(), columns, seen);
      collectRefs(bin.getRight(), columns, seen);
    } else if (expr instanceof ExprNode.UnaryExpr) {
      collectRefs(((ExprNode.UnaryExpr) expr).getChild(), columns, seen);
    } else if (expr instanceof ExprNode.FieldAccess) {
      collectRefs(((ExprNode.FieldAccess) expr).getObject(), columns, seen);
    } else if (expr instanceof ExprNode.FunctionCall) {
      for (ExprNode arg : ((ExprNode.FunctionCall) expr).getArgs()) {
        collectRefs(arg, columns, seen);
      }
    } else if (expr instanceof ExprNode.CastExpr) {
      collectRefs(((ExprNode.CastExpr) expr).getChild(), columns, seen);
    } else if (expr instanceof ExprNode.IfExpr) {
      ExprNode.IfExpr ifExpr = (ExprNode.IfExpr) expr;
      collectRefs(ifExpr.getCondition(), columns, seen);
      collectRefs(ifExpr.getTrueValue(), columns, seen);
      collectRefs(ifExpr.getFalseValue(), columns, seen);
    } else if (expr instanceof ExprNode.CaseWhenExpr) {
      ExprNode.CaseWhenExpr cw = (ExprNode.CaseWhenExpr) expr;
      for (ExprNode[] branch : cw.getBranches()) {
        collectRefs(branch[0], columns, seen);
        collectRefs(branch[1], columns, seen);
      }
      collectRefs(cw.getElseValue(), columns, seen);
    } else if (expr instanceof ExprNode.InExpr) {
      ExprNode.InExpr in = (ExprNode.InExpr) expr;
      collectRefs(in.getValue(), columns, seen);
      for (ExprNode item : in.getList()) {
        collectRefs(item, columns, seen);
      }
    }
  }

  public static class Result {
    private final List<ColumnIR> columns;
    private final int truncatedCount;

    public Result(List<ColumnIR> columns, int truncatedCount) {
      this.columns = columns;
      this.truncatedCount = truncatedCount;
    }

    public List<ColumnIR> getColumns() {
      return columns;
    }

    public int getTruncatedCount() {
      return truncatedCount;
    }
  }
}
