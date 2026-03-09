/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.environment;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;

/**
 * Factory for the default set of {@link Interception}s that make the Spark environment cheap to
 * construct during benchmarking.
 *
 * <h2>Default interceptions</h2>
 * <ul>
 *   <li><b>QueryExecution.optimizedPlan()/analyzed()</b> — return {@code logical} directly
 *       (skips Catalyst analyzer + optimizer; most impactful for large plan trees)</li>
 *   <li><b>QueryExecution.executedPlan()/sparkPlan()</b> — return a {@code SyntheticSparkPlan}
 *       stub (skips physical planning; satisfies OL's job-naming calls)</li>
 *   <li><b>InMemoryFileIndex.inputFiles()</b> — return empty array
 *       (prevents accidental filesystem scans)</li>
 *   <li><b>InMemoryFileIndex.sizeInBytes()</b> — return 0L
 *       (prevents accidental filesystem scans)</li>
 *   <li><b>FileSystem.get(URI, Configuration)</b> — return {@code SyntheticHadoopFileSystem}
 *       for {@code synthetic-cluster} URIs (prevents real HDFS connection attempts)</li>
 * </ul>
 *
 * <p>Tests can start with these defaults and layer additional interceptions via
 * {@link SyntheticEnvironment#add(Interception)}.
 *
 * <p>Direct type references to {@code QueryExecution} and {@code LogicalPlan} are safe here
 * because handlers are regular Java lambdas — not bytecode inlined into target classes.
 * With {@code InitializationStrategy.NoOp}, the dispatcher Advice only references
 * {@code InterceptionDispatcher}, which is never in a circular class-loading chain.
 */
public class DefaultRules {

  private static final String QUERY_EXECUTION =
      "org.apache.spark.sql.execution.QueryExecution";
  private static final String INMEMORY_FILE_INDEX =
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex";
  private static final String FILE_SYSTEM = "org.apache.hadoop.fs.FileSystem";
  private static final String SYNTHETIC_SPARK_PLAN =
      "io.openlineage.spark.harness.synthetic.SyntheticSparkPlan";
  private static final String SYNTHETIC_HADOOP_FS =
      "io.openlineage.spark.harness.synthetic.SyntheticHadoopFileSystem";

  private DefaultRules() {}

  /**
   * Create the default interception set.
   *
   * @return list of interceptions to pass to {@link SyntheticEnvironment}
   */
  public static List<Interception> create() {
    return Arrays.asList(
        // QueryExecution.optimizedPlan() / analyzed() → return logical if already resolved.
        // Skips the full Catalyst analyze + optimize pipeline for synthetic pre-resolved plans.
        // Spark's own unresolved plans are not intercepted (guard returns null for them).
        Interception.on(QUERY_EXECUTION, "optimizedPlan", "analyzed")
            .handle(
                ctx -> {
                  QueryExecution qe = ctx.self();
                  LogicalPlan plan = qe.logical();
                  return (plan != null && plan.resolved()) ? plan : null;
                }),

        // QueryExecution.executedPlan() / sparkPlan() → SyntheticSparkPlan stub.
        // Skips physical planning. Guard ensures only pre-resolved synthetic plans are
        // intercepted; Spark's own internal unresolved plans pass through unmodified.
        Interception.on(QUERY_EXECUTION, "executedPlan", "sparkPlan")
            .guard(
                ctx -> {
                  QueryExecution qe = ctx.self();
                  LogicalPlan plan = qe.logical();
                  return plan != null && plan.resolved();
                })
            .returnStub(SYNTHETIC_SPARK_PLAN, "harness-stub"),

        // InMemoryFileIndex.inputFiles() → empty array.
        Interception.on(INMEMORY_FILE_INDEX, "inputFiles").returnConstant(new String[0]),

        // InMemoryFileIndex.sizeInBytes() → 0L.
        Interception.on(INMEMORY_FILE_INDEX, "sizeInBytes").returnConstant(0L),

        // FileSystem.get(URI, Configuration) → SyntheticHadoopFileSystem for synthetic-cluster.
        // Guard filters by URI host so only synthetic HDFS paths are intercepted.
        Interception.onStatic(FILE_SYSTEM, "get")
            .guard(
                ctx -> {
                  URI uri = ctx.arg(0);
                  return uri != null && "synthetic-cluster".equals(uri.getHost());
                })
            .returnStub(SYNTHETIC_HADOOP_FS));
  }
}
