/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

class FaithfulPlanBuilderTest {

  private static SparkSession spark;
  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeAll
  static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("FaithfulPlanBuilderTest")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void buildsSimplePlan() throws IOException {
    PlanIR planIR = loadResource("simple-plan.plan.json");

    LogicalPlan plan = FaithfulPlanBuilder.build(spark, planIR);

    // Root should be Project
    assertThat(plan).isInstanceOf(Project.class);
    Project project = (Project) plan;
    assertThat(project.output().size()).isEqualTo(2);

    // Child should be Filter
    assertThat(project.child()).isInstanceOf(Filter.class);
    Filter filter = (Filter) project.child();

    // Filter's child should be LogicalRelation
    assertThat(filter.child()).isInstanceOf(LogicalRelation.class);
    LogicalRelation relation = (LogicalRelation) filter.child();
    assertThat(relation.output().size()).isEqualTo(3);
  }

  @Test
  void buildsJoinPlan() throws IOException {
    PlanIR planIR = loadResource("join-plan.plan.json");

    LogicalPlan plan = FaithfulPlanBuilder.build(spark, planIR);

    // Root is Project
    assertThat(plan).isInstanceOf(Project.class);
    Project root = (Project) plan;
    assertThat(root.output().size()).isEqualTo(3);

    // Project wraps a Join (which itself is wrapped in another Project by buildJoin)
    LogicalPlan joinOrProject = root.child();
    // buildJoin wraps Join in a Project
    assertThat(joinOrProject).isInstanceOf(Project.class);
    Project joinProject = (Project) joinOrProject;

    // The Join is the child of that inner Project
    assertThat(joinProject.child()).isInstanceOf(Join.class);
    Join join = (Join) joinProject.child();
    assertThat(join.joinType().toString()).contains("LeftOuter");

    // Left child of join should be SubqueryAlias
    assertThat(join.left()).isInstanceOf(SubqueryAlias.class);
    SubqueryAlias alias = (SubqueryAlias) join.left();
    assertThat(alias.identifier().name()).isEqualTo("users");
  }

  @Test
  @EnabledIf("largePlanExists")
  void buildsLargePlan() throws IOException {
    Path largePlanPath = Paths.get("/tmp/meta-event.plan.json");
    PlanIR planIR = mapper.readValue(largePlanPath.toFile(), PlanIR.class);

    assertThat(planIR.nodeCount()).isGreaterThan(100);

    LogicalPlan plan = FaithfulPlanBuilder.build(spark, planIR);

    assertThat(plan).isNotNull();
    // The plan should have output columns
    assertThat(plan.output().size()).isGreaterThan(0);

    // Verify we can call treeString without errors — this exercises the full plan tree
    String treeString = plan.treeString();
    assertThat(treeString).isNotEmpty();
    assertThat(treeString.split("\n").length).isGreaterThan(100);
  }

  static boolean largePlanExists() {
    return Files.exists(Paths.get("/tmp/meta-event.plan.json"));
  }

  private PlanIR loadResource(String name) throws IOException {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(name)) {
      assertThat(is).as("Resource " + name + " should exist").isNotNull();
      return mapper.readValue(is, PlanIR.class);
    }
  }
}
