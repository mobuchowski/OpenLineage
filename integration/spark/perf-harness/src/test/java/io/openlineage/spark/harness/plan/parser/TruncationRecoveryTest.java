/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.plan.parser;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.spark.harness.plan.ir.ColumnIR;
import io.openlineage.spark.harness.plan.ir.NodeIR;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class TruncationRecoveryTest {

  private static ColumnIR col(String name, long id) {
    return new ColumnIR(name, id, "string");
  }

  private static NodeIR node(int id, String type, List<ColumnIR> cols, int truncated,
      Integer... children) {
    NodeIR n = new NodeIR(id, type);
    n.setOutput(new ArrayList<>(cols));
    n.setTruncatedColumns(truncated);
    n.setChildren(new ArrayList<>(Arrays.asList(children)));
    return n;
  }

  @Test
  void synthesizesColumnsForLeafRelation() {
    PlanIR plan = new PlanIR();
    NodeIR relation = node(0, "Relation",
        Arrays.asList(col("a", 1), col("b", 2)), 3);
    plan.setNodes(Arrays.asList(relation));
    plan.setRootNodeId(0);

    int recovered = TruncationRecovery.recover(plan);

    assertThat(recovered).isEqualTo(3);
    assertThat(relation.getOutput()).hasSize(5);
    assertThat(relation.getTruncatedColumns()).isEqualTo(0);
    // First 2 are original
    assertThat(relation.getOutput().get(0).getName()).isEqualTo("a");
    assertThat(relation.getOutput().get(1).getName()).isEqualTo("b");
    // Last 3 are synthetic
    assertThat(relation.getOutput().get(2).getName()).isEqualTo("_col2");
    assertThat(relation.getOutput().get(3).getName()).isEqualTo("_col3");
    assertThat(relation.getOutput().get(4).getName()).isEqualTo("_col4");
  }

  @Test
  void propagatesFromChild() {
    // Project with 2 visible + 1 truncated, child has all 3
    NodeIR child = node(1, "Relation",
        Arrays.asList(col("a", 1), col("b", 2), col("c", 3)), 0);
    NodeIR project = node(0, "Project",
        Arrays.asList(col("a", 1), col("b", 2)), 1, 1);

    PlanIR plan = new PlanIR();
    plan.setNodes(Arrays.asList(project, child));
    plan.setRootNodeId(0);

    int recovered = TruncationRecovery.recover(plan);

    assertThat(recovered).isEqualTo(1);
    assertThat(project.getOutput()).hasSize(3);
    assertThat(project.getTruncatedColumns()).isEqualTo(0);
    assertThat(project.getOutput().get(2).getName()).isEqualTo("c");
    assertThat(project.getOutput().get(2).getExprId()).isEqualTo(3);
  }

  @Test
  void synthesizesWhenChildAlsoTruncated() {
    // Both parent and child truncated — can't propagate, must synthesize
    NodeIR child = node(1, "Relation",
        Arrays.asList(col("a", 1)), 2);
    NodeIR project = node(0, "Project",
        Arrays.asList(col("a", 1)), 2, 1);

    PlanIR plan = new PlanIR();
    plan.setNodes(Arrays.asList(project, child));
    plan.setRootNodeId(0);

    int recovered = TruncationRecovery.recover(plan);

    // 2 synthesized for child + 2 synthesized for project = 4
    assertThat(recovered).isEqualTo(4);
    assertThat(child.getOutput()).hasSize(3);
    assertThat(child.getTruncatedColumns()).isEqualTo(0);
    assertThat(project.getOutput()).hasSize(3);
    assertThat(project.getTruncatedColumns()).isEqualTo(0);
  }

  @Test
  void syntheticIdsDoNotCollide() {
    NodeIR relation = node(0, "Relation",
        Arrays.asList(col("a", 100), col("b", 200)), 2);

    PlanIR plan = new PlanIR();
    plan.setNodes(Arrays.asList(relation));
    plan.setRootNodeId(0);

    TruncationRecovery.recover(plan);

    // Synthetic IDs should be > 200
    assertThat(relation.getOutput().get(2).getExprId()).isGreaterThan(200);
    assertThat(relation.getOutput().get(3).getExprId()).isGreaterThan(200);
    // And sequential
    assertThat(relation.getOutput().get(3).getExprId())
        .isEqualTo(relation.getOutput().get(2).getExprId() + 1);
  }

  @Test
  void noOpWhenNoTruncation() {
    NodeIR relation = node(0, "Relation",
        Arrays.asList(col("a", 1), col("b", 2)), 0);

    PlanIR plan = new PlanIR();
    plan.setNodes(Arrays.asList(relation));
    plan.setRootNodeId(0);

    int recovered = TruncationRecovery.recover(plan);

    assertThat(recovered).isEqualTo(0);
    assertThat(relation.getOutput()).hasSize(2);
  }

  @Test
  void propagatesBeforeSynthesizing() {
    // Chain: Project(2+1 trunc) → Filter(3 cols, no trunc) → Relation(3 cols, no trunc)
    // Project should propagate from Filter, not synthesize
    NodeIR relation = node(2, "Relation",
        Arrays.asList(col("a", 1), col("b", 2), col("c", 3)), 0);
    NodeIR filter = node(1, "Filter",
        Arrays.asList(col("a", 1), col("b", 2), col("c", 3)), 0, 2);
    NodeIR project = node(0, "Project",
        Arrays.asList(col("a", 1), col("b", 2)), 1, 1);

    PlanIR plan = new PlanIR();
    plan.setNodes(Arrays.asList(project, filter, relation));
    plan.setRootNodeId(0);

    TruncationRecovery.recover(plan);

    // Project recovered from Filter — real column name, not synthetic
    assertThat(project.getOutput()).hasSize(3);
    assertThat(project.getOutput().get(2).getName()).isEqualTo("c");
    assertThat(project.getTruncatedColumns()).isEqualTo(0);
  }
}
