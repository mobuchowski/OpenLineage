/** 
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.generator

import io.openlineage.spark.harness.synthetic.SyntheticFileIndex
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Builds synthetic resolved LogicalPlan trees for use in the performance harness.
 *
 * The generated plans mimic the structural characteristics of real production pipelines
 * (node count, tree depth, schema width, file count) without any real data or filesystem access.
 *
 * <h2>Algorithm</h2>
 * <ol>
 *   <li>Create {@code leafCount} {@code LogicalRelation} nodes each backed by the same
 *       {@link SyntheticFileIndex}.</li>
 *   <li>Combine leaves bottom-up into a binary tree using alternating Union and Join+Project
 *       nodes to keep the output schema stable.</li>
 *   <li>Wrap the root with a chain of {@code Project} and {@code Filter} nodes until the
 *       target depth is reached.</li>
 * </ol>
 *
 * <h2>Usage (Scala)</h2>
 * {{{
 *   // Pre-built GCP billing pipeline replica
 *   val plan = PlanGenerator.gcpBillingPipeline(spark)
 *
 *   // Custom plan
 *   val plan = PlanGenerator.build(spark,
 *     leafCount   = 310,
 *     fileCount   = 331_000,
 *     schemaWidth = 500,
 *     targetDepth = 134)
 * }}}
 *
 * <h2>Usage (Java)</h2>
 * {{{
 *   LogicalPlan plan = PlanGenerator$.MODULE$.gcpBillingPipeline(spark);
 * }}}
 */
object PlanGenerator {

  /**
   * Build a plan matching the GCP billing pipeline from the performance audit:
   * 6,778 total nodes, 310 leaves, 331,000 files per leaf, 500-column schema, depth 134.
   */
  def gcpBillingPipeline(spark: SparkSession): LogicalPlan =
    build(spark, leafCount = 310, fileCount = 331000, schemaWidth = 500, targetDepth = 134)

  /**
   * Minimal single-relation plan for smoke testing and hypothesis validation.
   * One LogicalRelation leaf → triggers isSingleFileRelation() once. Runs in seconds.
   */
  def singleRelation(spark: SparkSession): LogicalPlan =
    build(spark, leafCount = 1, fileCount = 10, schemaWidth = 5, targetDepth = 1)

  /**
   * Build a synthetic resolved LogicalPlan.
   *
   * @param spark       SparkSession (required for HadoopFsRelation schema merging)
   * @param leafCount   number of LogicalRelation leaf nodes
   * @param fileCount   synthetic files per leaf (reported by SyntheticFileIndex.inputFiles())
   * @param schemaWidth number of StringType columns in each leaf's schema
   * @param targetDepth target tree depth (layers of Project/Filter added above the binary tree)
   */
  def build(
      spark: SparkSession,
      leafCount: Int,
      fileCount: Int,
      schemaWidth: Int,
      targetDepth: Int): LogicalPlan = {
    require(leafCount > 0, "leafCount must be positive")
    require(schemaWidth > 0, "schemaWidth must be positive")
    require(targetDepth > 0, "targetDepth must be positive")

    val schema = NodeFactory.makeSchema(schemaWidth)
    // Share one SyntheticFileIndex instance across all leaves — it holds the pre-generated paths.
    val fileIndex = new SyntheticFileIndex(fileCount)

    val leaves = (0 until leafCount)
      .map(_ => NodeFactory.createLeafRelation(spark, schema, fileIndex))
      .toList

    val tree = buildBinaryTree(leaves, level = 0)
    deepen(tree, targetDepth)
  }

  /**
   * Recursively combine a list of plans into a single binary tree.
   *
   * At each level, adjacent plan pairs are combined using either Union or Join+Project.
   * Union keeps the schema identical across siblings (required by Spark). Join+Project
   * keeps the output schema stable (projects back to the left child's columns).
   *
   * Alternation strategy: level % 3:
   *  - 0, 1 → Union  (most common in real large pipelines)
   *  - 2    → Join+Project (introduces Join nodes without schema explosion)
   */
  private def buildBinaryTree(plans: List[LogicalPlan], level: Int): LogicalPlan =
    plans match {
      case Nil         => throw new IllegalArgumentException("plans must be non-empty")
      case List(one)   => one
      case _           =>
        val combined = plans.grouped(2).zipWithIndex.map { case (pair, idx) =>
          pair match {
            case List(single) => single
            case List(l, r)   =>
              if ((level + idx) % 3 == 2)
                NodeFactory.createJoin(l, r)   // Join + Project → stable schema
              else
                NodeFactory.createUnion(Seq(l, r))  // Union → same schema required
            case _ => throw new IllegalStateException("grouped(2) returned >2 elements")
          }
        }.toList
        buildBinaryTree(combined, level + 1)
    }

  /**
   * Extend the plan's depth by prepending Project and Filter nodes until {@code targetDepth}
   * is reached. The extra nodes are a no-op for OL lineage purposes (pass-through Project,
   * trivially-true Filter) but they exercise the visitor traversal path.
   */
  private def deepen(plan: LogicalPlan, targetDepth: Int): LogicalPlan = {
    val current = treeDepth(plan)
    val needed = targetDepth - current
    var result = plan
    for (i <- 0 until math.max(0, needed)) {
      result = if (i % 2 == 0) NodeFactory.createProject(result)
               else             NodeFactory.createFilter(result)
    }
    result
  }

  /** Compute the maximum depth of the tree (root = depth 1). */
  private def treeDepth(plan: LogicalPlan): Int =
    if (plan.children.isEmpty) 1
    else 1 + plan.children.map(treeDepth).max
}
