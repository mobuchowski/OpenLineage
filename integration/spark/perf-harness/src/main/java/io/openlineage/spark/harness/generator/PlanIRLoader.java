/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.generator;

import io.openlineage.spark.harness.plan.ir.NodeIR;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Converts a {@link PlanIR} (intermediate representation) back into a synthetic Spark LogicalPlan.
 *
 * <p>Rather than reconstructing the exact plan topology, this builds a structurally equivalent
 * synthetic plan via {@link PlanGenerator$}. The generated plan matches the IR's leaf count,
 * schema width, and tree depth — sufficient for OL performance testing.
 */
public class PlanIRLoader {

  private PlanIRLoader() {}

  /**
   * Build a synthetic LogicalPlan from a PlanIR.
   *
   * <p>Derives structural parameters from the IR:
   * <ul>
   *   <li>leafCount: number of nodes with no children</li>
   *   <li>schemaWidth: average output column count across all nodes (min 1)</li>
   *   <li>targetDepth: maximum depth of the IR tree</li>
   * </ul>
   *
   * @param spark  SparkSession for HadoopFsRelation construction
   * @param planIR intermediate representation parsed from a captured plan
   * @return a synthetic resolved LogicalPlan with equivalent structural characteristics
   */
  public static LogicalPlan load(SparkSession spark, PlanIR planIR) {
    java.util.List<NodeIR> nodes = planIR.getNodes();
    if (nodes.isEmpty()) {
      throw new IllegalArgumentException("PlanIR has no nodes");
    }

    int leafCount = Math.max(1,
        (int) nodes.stream().filter(n -> n.getChildren().isEmpty()).count());

    int totalCols = nodes.stream()
        .mapToInt(n -> n.getOutput().size() + n.getTruncatedColumns())
        .sum();
    int schemaWidth = Math.max(1, totalCols / nodes.size());

    int targetDepth = Math.max(1, planIR.maxDepth());

    return PlanGenerator$.MODULE$.build(spark, leafCount, 1, schemaWidth, targetDepth);
  }
}
