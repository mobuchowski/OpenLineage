/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness;

import io.openlineage.spark.harness.environment.SyntheticEnvironment;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * A single performance harness scenario.
 *
 * <p>Implementations define the synthetic {@link LogicalPlan} to benchmark and can customize the
 * {@link SyntheticEnvironment} (e.g. override interception rules, adjust file counts).
 *
 * <p>The harness has NO compile-time dependency on OL. The OL JAR is loaded at runtime from a
 * configurable path and registered as a Spark listener by class name. This interface references
 * only Spark classes (LogicalPlan, SparkSession) and harness classes (SyntheticEnvironment).
 *
 * <p>Example:
 * <pre>{@code
 * public class GcpBillingPipelineTest implements HarnessTest {
 *     public LogicalPlan generatePlan(SparkSession spark) {
 *         return PlanGenerator$.MODULE$.gcpBillingPipeline(spark);
 *     }
 * }
 * }</pre>
 */
public interface HarnessTest {

  /**
   * Override environment rules for this test.
   *
   * <p>The default implementation does nothing, keeping the {@link SyntheticEnvironment#withDefaults()
   * default rules} active.
   */
  default void configure(SyntheticEnvironment env) {}

  /**
   * Generate the LogicalPlan to benchmark.
   *
   * <p>The plan must be resolved (valid {@code AttributeReference}s with types), because OL
   * visitors call {@code .schema()}, {@code .output()}, and {@code .references()} on plan nodes.
   *
   * <p>The SparkSession is passed explicitly so implementations can call
   * {@code PlanGenerator$.MODULE$.gcpBillingPipeline(spark)} or use other factory methods
   * that require a live session (e.g., for HadoopFsRelation schema merging).
   *
   * @param spark the SparkSession created by {@link HarnessRunner}
   */
  LogicalPlan generatePlan(SparkSession spark);
}
