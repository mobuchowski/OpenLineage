/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.tests;

import io.openlineage.spark.harness.HarnessTest;
import io.openlineage.spark.harness.environment.SyntheticEnvironment;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Minimal smoke test that verifies the SyntheticHadoopFileSystemRule is working.
 *
 * <p>Generates a plan with a SINGLE LogicalRelation (vs 310 in GcpBillingPipelineTest) so
 * the run completes in seconds. Used to validate hypotheses about ByteBuddy class interception
 * and Path.getFileSystem() without waiting for the full benchmark.
 *
 * <p>Expected outcome: no "Failed to initialize filesystem hdfs://synthetic-cluster" WARN in logs.
 */
public class PathInterceptionSmokeTest implements HarnessTest {

  @Override
  public LogicalPlan generatePlan(SparkSession spark) {
    // Single-table scan — one LogicalRelation, one SyntheticFileIndex, one rootPath call.
    // Enough to trigger isSingleFileRelation() without the 310-leaf overhead.
    return io.openlineage.spark.harness.generator.PlanGenerator$.MODULE$
        .singleRelation(spark);
  }

  @Override
  public void configure(SyntheticEnvironment env) {
    // Use default rules — same setup as GcpBillingPipelineTest
  }
}
