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
import io.openlineage.spark.harness.generator.FaithfulPlanBuilder;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import io.openlineage.spark.harness.plan.parser.SparkJsonParser;
import io.openlineage.spark.harness.plan.parser.TreeStringParser;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Harness test that loads a LogicalPlan from a serialized plan file or pre-converted IR.
 *
 * <p>Supports three input formats:
 * <ul>
 *   <li>Plan IR JSON ({@code .plan.json}) — pre-converted intermediate representation</li>
 *   <li>Spark treeString text ({@code .txt}) — output of {@code df.explain()} or logs</li>
 *   <li>Spark JSON ({@code .json}) — output of {@code plan.toJSON()}</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * // From a pre-converted IR file:
 * HarnessTest test = new IRPlanTest("/path/to/plan.plan.json");
 *
 * // From a treeString text file (parsed on the fly):
 * HarnessTest test = new IRPlanTest("/path/to/plan.txt");
 *
 * // Then run via HarnessRunner as usual.
 * }</pre>
 */
public class IRPlanTest implements HarnessTest {

  private final PlanIR planIR;
  private final String sourcePath;

  public IRPlanTest(String path) throws IOException {
    this.sourcePath = path;
    this.planIR = loadPlanIR(Paths.get(path));
  }

  public IRPlanTest(PlanIR planIR) {
    this.sourcePath = "<in-memory>";
    this.planIR = planIR;
  }

  @Override
  public LogicalPlan generatePlan(SparkSession spark) {
    return FaithfulPlanBuilder.build(spark, planIR);
  }

  public PlanIR getPlanIR() {
    return planIR;
  }

  @Override
  public String toString() {
    return "IRPlanTest{source=" + sourcePath
        + ", nodes=" + planIR.nodeCount()
        + ", depth=" + planIR.maxDepth() + "}";
  }

  private static PlanIR loadPlanIR(Path path) throws IOException {
    String filename = path.getFileName().toString().toLowerCase();

    if (filename.endsWith(".plan.json")) {
      // Pre-converted IR format
      com.fasterxml.jackson.databind.ObjectMapper mapper =
          new com.fasterxml.jackson.databind.ObjectMapper();
      return mapper.readValue(path.toFile(), PlanIR.class);
    } else if (filename.endsWith(".json")) {
      // Spark's toJSON() format
      return new SparkJsonParser().parseFile(path);
    } else {
      // Default: treeString text format
      return new TreeStringParser().parseFile(path);
    }
  }
}
