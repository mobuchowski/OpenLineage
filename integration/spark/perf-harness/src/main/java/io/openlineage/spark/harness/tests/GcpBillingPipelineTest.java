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
import io.openlineage.spark.harness.generator.PlanGenerator$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Harness test that reproduces the GCP billing pipeline shape from the performance audit.
 *
 * <p>Plan characteristics:
 * <ul>
 *   <li>6,778 total nodes (generated via PlanGenerator binary tree + depth extension)</li>
 *   <li>310 LogicalRelation leaves</li>
 *   <li>331,000 synthetic file paths per leaf (via SyntheticFileIndex)</li>
 *   <li>500-column schema (StringType columns)</li>
 *   <li>Target tree depth: 134</li>
 * </ul>
 *
 * <p>This is the baseline measurement for all OL optimization work. Run before and after each
 * optimization to validate performance improvements in Datadog APM.
 *
 * <p>Usage:
 * <pre>{@code
 * // From Gradle:
 * ./gradlew :perf-harness:runHarness
 *
 * // With specific OL version:
 * ./gradlew :perf-harness:runHarness -PolJar=/path/to/openlineage-spark_2.13-1.30.0.jar
 * }</pre>
 */
public class GcpBillingPipelineTest implements HarnessTest {

  @Override
  public LogicalPlan generatePlan(SparkSession spark) {
    return PlanGenerator$.MODULE$.gcpBillingPipeline(spark);
  }
}
