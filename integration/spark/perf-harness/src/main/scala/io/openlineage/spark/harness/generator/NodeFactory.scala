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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types._

/**
 * Factory methods for creating synthetic Spark LogicalPlan nodes.
 *
 * All nodes are fully resolved — AttributeReferences carry valid ExprIds and DataTypes so that
 * OL visitors can call .schema(), .output(), and .references() without errors.
 *
 * Usage from Scala:
 * {{{
 *   val schema = NodeFactory.makeSchema(500)
 *   val fileIndex = new SyntheticFileIndex(331_000)
 *   val leaf = NodeFactory.createLeafRelation(spark, schema, fileIndex)
 *   val filtered = NodeFactory.createFilter(leaf)
 *   val projected = NodeFactory.createProject(filtered)
 * }}}
 *
 * Usage from Java via the companion object's MODULE$:
 * {{{
 *   NodeFactory$.MODULE$.createLeafRelation(spark, schema, fileIndex)
 * }}}
 */
object NodeFactory {

  /**
   * Generate a StructType with {@code width} nullable StringType columns named col_0 … col_{n-1}.
   */
  def makeSchema(width: Int): StructType =
    StructType((0 until width).map(i => StructField(s"col_$i", StringType, nullable = true)))

  /**
   * Create a resolved LogicalRelation backed by a HadoopFsRelation + SyntheticFileIndex.
   *
   * OL visitors that match LogicalRelation call:
   *  - relation.location.inputFiles()     — SyntheticFileIndex returns synthetic paths
   *  - relation.location.sizeInBytes()    — SyntheticFileIndex returns 128 MB * fileCount
   *  - output schema / AttributeReferences — generated from the provided StructType
   */
  def createLeafRelation(
      spark: SparkSession,
      schema: StructType,
      fileIndex: SyntheticFileIndex): LogicalRelation = {
    val relation = HadoopFsRelation(
      location = fileIndex,
      partitionSchema = StructType(Nil),
      dataSchema = schema,
      bucketSpec = None,
      fileFormat = new ParquetFileFormat,
      options = Map.empty)(spark)
    // Generate resolved AttributeReferences — each has a unique auto-assigned ExprId.
    val output: Seq[AttributeReference] =
      schema.fields.map(f => AttributeReference(f.name, f.dataType, nullable = true)()).toSeq
    LogicalRelation(
      relation = relation,
      output = output,
      catalogTable = None,
      isStreaming = false,
      stream = None)
  }

  /**
   * Wrap {@code child} in a pass-through Project (output = child.output).
   * Represents SELECT * — a no-op for lineage purposes but adds a node to the tree.
   */
  def createProject(child: LogicalPlan): Project =
    Project(child.output, child)

  /**
   * Wrap {@code child} in a trivially true Filter (WHERE TRUE).
   * OL visitors visit Filter nodes; this satisfies them without real predicate logic.
   */
  def createFilter(child: LogicalPlan): Filter =
    Filter(Literal(true, BooleanType), child)

  /**
   * Combine two plans with an inner Join, then Project down to the left schema.
   *
   * The Project after Join keeps the output schema stable (avoids exponential column growth
   * when joining in a tree). The join condition is None (cross join — acceptable for a
   * synthetic plan since OL doesn't execute the plan).
   */
  def createJoin(left: LogicalPlan, right: LogicalPlan): Project = {
    val joined = Join(left, right, Inner, None, JoinHint.NONE)
    Project(left.output, joined)
  }

  /**
   * Combine two or more plans with a Union.
   *
   * Requires all children to have the same schema (guaranteed when all leaves use the
   * same {@link NodeFactory#makeSchema} result). The Union output uses byName=false
   * (position-based matching).
   */
  def createUnion(children: Seq[LogicalPlan]): Union =
    Union(children)

  /**
   * Wrap {@code child} in an Aggregate (GROUP BY first column, SELECT first column).
   * Produces a single-column output for downstream nodes to project over.
   */
  def createAggregate(child: LogicalPlan): Aggregate = {
    val groupExpr = child.output.headOption.toSeq
    Aggregate(groupExpr, groupExpr, child)
  }
}
