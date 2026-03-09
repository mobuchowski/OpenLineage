/** 
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.synthetic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Minimal SparkPlan stub returned by QueryExecution interceptions.
 *
 * OL accesses executedPlan() primarily for job naming (nodeName()) and filtering
 * decisions. This leaf node satisfies those calls without triggering any Spark execution.
 *
 * Instantiated via reflection from QueryExecutionExecutedPlanRule to avoid a
 * compile-time dependency between the Java rule classes and the Scala bridge JAR.
 *
 * @param nameHint label embedded in nodeName, used for logging and OL job naming
 */
class SyntheticSparkPlan(val nameHint: String) extends LeafExecNode {

  override def nodeName: String = s"SyntheticSparkPlan[$nameHint]"

  override def output: Seq[Attribute] = Seq.empty

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(
      "SyntheticSparkPlan is a harness stub — doExecute() is not supported")

  // Product trait implementation required by TreeNode in Spark 4
  override def canEqual(that: Any): Boolean = that.isInstanceOf[SyntheticSparkPlan]
  override def productArity: Int = 1
  override def productElement(n: Int): Any =
    if (n == 0) nameHint else throw new IndexOutOfBoundsException(n.toString)
}
