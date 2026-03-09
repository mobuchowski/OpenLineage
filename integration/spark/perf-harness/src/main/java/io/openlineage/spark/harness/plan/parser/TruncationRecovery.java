/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.parser;

import io.openlineage.spark.harness.plan.ir.ColumnIR;
import io.openlineage.spark.harness.plan.ir.NodeIR;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Post-processing pass that recovers truncated columns in a {@link PlanIR}.
 *
 * <p>Spark's {@code treeString()} truncates column lists at {@code SQLConf.MAX_TO_STRING_FIELDS}
 * (default 25). This pass recovers the missing columns using two strategies:
 *
 * <ol>
 *   <li><b>Bottom-up propagation</b>: For interior nodes (Project, Filter, etc.), if a descendant
 *       has a non-truncated column list that is a superset, copy the missing columns from there.</li>
 *   <li><b>Synthesis</b>: For leaf nodes (Relations) and any remaining truncated nodes, generate
 *       synthetic placeholder columns to preserve the correct schema width. Synthetic columns use
 *       names like {@code _col25}, {@code _col26}, etc. and sequential exprIds starting above
 *       the maximum exprId in the plan.</li>
 * </ol>
 *
 * <p>For the performance harness, schema width is what matters — the actual names of truncated
 * columns don't affect OL traversal performance.
 */
public class TruncationRecovery {

  private static final Logger log = LoggerFactory.getLogger(TruncationRecovery.class);

  private TruncationRecovery() {}

  /**
   * Run truncation recovery on a plan, modifying it in place.
   *
   * @return the number of columns recovered or synthesized
   */
  public static int recover(PlanIR plan) {
    Map<Integer, NodeIR> byId = new HashMap<>();
    for (NodeIR node : plan.getNodes()) {
      byId.put(node.getId(), node);
    }

    // Find the max exprId so synthetic IDs don't collide
    long maxExprId = findMaxExprId(plan);
    long nextSyntheticId = maxExprId + 1;

    int propagated = 0;
    int synthesized = 0;

    // Pass 1: bottom-up propagation.
    // Process nodes in reverse order (children before parents) so that
    // children are already recovered when we process their parents.
    List<NodeIR> nodesReversed = new ArrayList<>(plan.getNodes());
    java.util.Collections.reverse(nodesReversed);

    for (NodeIR node : nodesReversed) {
      if (node.getTruncatedColumns() <= 0) {
        continue;
      }

      int needed = node.getTruncatedColumns();
      List<ColumnIR> recovered = tryPropagate(node, byId);

      if (!recovered.isEmpty()) {
        List<ColumnIR> merged = new ArrayList<>(node.getOutput());
        merged.addAll(recovered);
        node.setOutput(merged);
        int actualRecovered = Math.min(recovered.size(), needed);
        node.setTruncatedColumns(Math.max(0, needed - actualRecovered));
        propagated += actualRecovered;
      }
    }

    // Pass 2: synthesize remaining truncated columns
    for (NodeIR node : plan.getNodes()) {
      if (node.getTruncatedColumns() <= 0) {
        continue;
      }

      int needed = node.getTruncatedColumns();
      int startIdx = node.getOutput().size();
      List<ColumnIR> synthetic = new ArrayList<>();

      for (int i = 0; i < needed; i++) {
        String name = "_col" + (startIdx + i);
        synthetic.add(new ColumnIR(name, nextSyntheticId++, "string"));
      }

      List<ColumnIR> merged = new ArrayList<>(node.getOutput());
      merged.addAll(synthetic);
      node.setOutput(merged);
      node.setTruncatedColumns(0);
      synthesized += needed;
    }

    if (propagated > 0 || synthesized > 0) {
      log.info("Truncation recovery: {} propagated, {} synthesized", propagated, synthesized);
    }

    return propagated + synthesized;
  }

  /**
   * Try to recover truncated columns from a child node that has a superset of columns.
   *
   * <p>For nodes like Project, Filter, Sort — the child's output contains all columns that this
   * node could reference. If the child has more columns than this node's visible set, the extra
   * columns fill the truncated gap.
   */
  private static List<ColumnIR> tryPropagate(NodeIR node, Map<Integer, NodeIR> byId) {
    if (node.getChildren().isEmpty()) {
      return new ArrayList<>();
    }

    int totalExpected = node.getOutput().size() + node.getTruncatedColumns();

    // Collect visible exprIds so we don't duplicate
    java.util.Set<Long> visibleIds = new java.util.HashSet<>();
    for (ColumnIR col : node.getOutput()) {
      visibleIds.add(col.getExprId());
    }

    // For single-child nodes (Project, Filter, Sort, etc.), the child output is the source
    // For multi-child nodes (Union, Join), we can't reliably propagate
    if (node.getChildren().size() == 1) {
      NodeIR child = byId.get(node.getChildren().get(0));
      if (child != null && child.getTruncatedColumns() == 0) {
        return findExtraColumns(child.getOutput(), visibleIds, node.getTruncatedColumns());
      }
    }

    return new ArrayList<>();
  }

  /**
   * Find columns in the source list that are not in the visible set.
   */
  private static List<ColumnIR> findExtraColumns(
      List<ColumnIR> sourceColumns, java.util.Set<Long> visibleIds, int maxNeeded) {
    List<ColumnIR> extra = new ArrayList<>();
    for (ColumnIR col : sourceColumns) {
      if (!visibleIds.contains(col.getExprId())) {
        extra.add(col);
        if (extra.size() >= maxNeeded) {
          break;
        }
      }
    }
    return extra;
  }

  private static long findMaxExprId(PlanIR plan) {
    long max = 0;
    for (NodeIR node : plan.getNodes()) {
      for (ColumnIR col : node.getOutput()) {
        max = Math.max(max, col.getExprId());
      }
      for (ColumnIR col : node.getConditionColumns()) {
        max = Math.max(max, col.getExprId());
      }
    }
    return max;
  }
}
