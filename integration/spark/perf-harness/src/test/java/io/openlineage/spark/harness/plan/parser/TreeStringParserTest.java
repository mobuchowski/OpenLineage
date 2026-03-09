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

import io.openlineage.spark.harness.plan.ir.NodeIR;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class TreeStringParserTest {

  private final TreeStringParser parser = new TreeStringParser();

  @Test
  void parsesSimplePlan() throws IOException {
    String plan =
        "== Parsed Logical Plan ==\n"
            + "Project [col_a#100, col_b#101]\n"
            + "+- Filter (col_a#100 = 1)\n"
            + "   +- Relation[col_a#100,col_b#101,col_c#102L] parquet\n";

    PlanIR ir = parser.parseString(plan);

    assertThat(ir.nodeCount()).isEqualTo(3);
    assertThat(ir.maxDepth()).isEqualTo(3);

    NodeIR root = ir.getNode(0);
    assertThat(root.getType()).isEqualTo("Project");
    assertThat(root.getChildren()).containsExactly(1);

    NodeIR filter = ir.getNode(1);
    assertThat(filter.getType()).isEqualTo("Filter");
    assertThat(filter.getChildren()).containsExactly(2);

    NodeIR relation = ir.getNode(2);
    assertThat(relation.getType()).isEqualTo("Relation");
    assertThat(relation.getChildren()).isEmpty();
    assertThat(relation.getOutput()).hasSize(3);
    assertThat(relation.getOutput().get(0).getName()).isEqualTo("col_a");
    assertThat(relation.getOutput().get(0).getExprId()).isEqualTo(100);
    assertThat(relation.getOutput().get(2).getName()).isEqualTo("col_c");
    assertThat(relation.getOutput().get(2).getDataType()).isEqualTo("long");
    assertThat(relation.getAttributes().get("relationType")).isEqualTo("parquet");
  }

  @Test
  void parsesUnionWithMultipleChildren() throws IOException {
    String plan =
        "Union\n"
            + ":- Project [a#1]\n"
            + ":  +- Relation[a#1,b#2] parquet\n"
            + "+- Project [a#3]\n"
            + "   +- Relation[a#3,b#4] parquet\n";

    PlanIR ir = parser.parseString(plan);

    assertThat(ir.nodeCount()).isEqualTo(5);

    NodeIR union = ir.getNode(0);
    assertThat(union.getType()).isEqualTo("Union");
    assertThat(union.getChildren()).containsExactly(1, 3);

    NodeIR firstProject = ir.getNode(1);
    assertThat(firstProject.getType()).isEqualTo("Project");
    assertThat(firstProject.getChildren()).containsExactly(2);

    NodeIR secondProject = ir.getNode(3);
    assertThat(secondProject.getType()).isEqualTo("Project");
    assertThat(secondProject.getChildren()).containsExactly(4);
  }

  @Test
  void parsesJoinNode() throws IOException {
    String plan =
        "Join LeftOuter, (a#1 = b#2)\n"
            + ":- Relation[a#1] parquet\n"
            + "+- Relation[b#2] parquet\n";

    PlanIR ir = parser.parseString(plan);

    assertThat(ir.nodeCount()).isEqualTo(3);
    NodeIR join = ir.getNode(0);
    assertThat(join.getType()).isEqualTo("Join");
    assertThat(join.getAttributes().get("joinType")).isEqualTo("LeftOuter");
    assertThat(join.getChildren()).containsExactly(1, 2);
    assertThat(join.getConditionColumns()).hasSize(2);
    assertThat(join.getConditionColumns().get(0).getName()).isEqualTo("a");
    assertThat(join.getConditionColumns().get(0).getExprId()).isEqualTo(1);
    assertThat(join.getConditionColumns().get(1).getName()).isEqualTo("b");
    assertThat(join.getConditionColumns().get(1).getExprId()).isEqualTo(2);
  }

  @Test
  void parsesJoinWithComplexCondition() throws IOException {
    String plan =
        "Join LeftOuter, ((_2#4594.service_id = _1#4593.service_id) "
            + "&& (_2#4594.client_id = _1#4593.client_id))\n"
            + ":- Relation[a#1] parquet\n"
            + "+- Relation[b#2] parquet\n";

    PlanIR ir = parser.parseString(plan);

    NodeIR join = ir.getNode(0);
    assertThat(join.getConditionColumns()).hasSize(2);
    assertThat(join.getConditionColumns().stream().anyMatch(c -> c.getExprId() == 4594)).isTrue();
    assertThat(join.getConditionColumns().stream().anyMatch(c -> c.getExprId() == 4593)).isTrue();
  }

  @Test
  void parsesFilterWithCondition() throws IOException {
    String plan =
        "Filter ((collector_timestamp#7213L >= 1000) && (collector_timestamp#7213L <= 2000))\n"
            + "+- Relation[collector_timestamp#7213L] parquet\n";

    PlanIR ir = parser.parseString(plan);

    NodeIR filter = ir.getNode(0);
    assertThat(filter.getType()).isEqualTo("Filter");
    assertThat(filter.getConditionColumns()).hasSize(1);
    assertThat(filter.getConditionColumns().get(0).getName()).isEqualTo("collector_timestamp");
    assertThat(filter.getConditionColumns().get(0).getExprId()).isEqualTo(7213);
  }

  @Test
  void parsesInsertIntoHadoopCommand() throws IOException {
    String plan =
        "InsertIntoHadoopFsRelationCommand gs://bucket/path, false, "
            + "org.apache.spark.sql.avro.AvroFileFormat@abc, Map(), Overwrite, "
            + "[col_a#1, col_b#2, ... 16 more fields]\n"
            + "+- Relation[col_a#1,col_b#2] parquet\n";

    PlanIR ir = parser.parseString(plan);

    NodeIR root = ir.getNode(0);
    assertThat(root.getType()).isEqualTo("InsertIntoHadoopFsRelationCommand");
    assertThat(root.getAttributes().get("path")).isEqualTo("gs://bucket/path,");
    assertThat(root.getAttributes().get("format")).isEqualTo("avro");
    assertThat(root.getAttributes().get("mode")).isEqualTo("Overwrite");
    // After truncation recovery: 2 visible + 16 synthesized = 18 total, 0 truncated
    assertThat(root.getTruncatedColumns()).isEqualTo(0);
    assertThat(root.getOutput()).hasSize(18);
    assertThat(root.getOutput().get(0).getName()).isEqualTo("col_a");
    assertThat(root.getOutput().get(1).getName()).isEqualTo("col_b");
  }

  @Test
  void parsesSerializeFromObjectWithAliases() throws IOException {
    String plan =
        "SerializeFromObject [staticinvoke(class UTF8String, StringType, fromString, "
            + "input[0, MyClass, true].name, true, false) AS name#100, "
            + "input[0, MyClass, true].age AS age#101]\n"
            + "+- Relation[x#200] parquet\n";

    PlanIR ir = parser.parseString(plan);

    NodeIR serialize = ir.getNode(0);
    assertThat(serialize.getType()).isEqualTo("SerializeFromObject");
    // Should extract AS aliases as output columns
    assertThat(serialize.getOutput()).hasSizeGreaterThanOrEqualTo(2);
    assertThat(serialize.getOutput().stream().anyMatch(c -> c.getName().equals("name"))).isTrue();
    assertThat(serialize.getOutput().stream().anyMatch(c -> c.getName().equals("age"))).isTrue();
  }

  @Test
  void handlesTruncatedColumns() throws IOException {
    String plan = "Relation[a#1,b#2,c#3,... 10 more fields] parquet\n";

    PlanIR ir = parser.parseString(plan);

    NodeIR relation = ir.getNode(0);
    // After truncation recovery: 3 visible + 10 synthesized = 13 total
    assertThat(relation.getOutput()).hasSize(13);
    assertThat(relation.getTruncatedColumns()).isEqualTo(0);
    // Original columns preserved
    assertThat(relation.getOutput().get(0).getName()).isEqualTo("a");
    assertThat(relation.getOutput().get(1).getName()).isEqualTo("b");
    assertThat(relation.getOutput().get(2).getName()).isEqualTo("c");
    // Synthetic columns follow
    assertThat(relation.getOutput().get(3).getName()).isEqualTo("_col3");
  }

  @Test
  void computeDepthWorksCorrectly() {
    assertThat(TreeStringParser.computeDepth("Project [a#1]")).isEqualTo(0);
    assertThat(TreeStringParser.computeDepth("+- Filter true")).isEqualTo(1);
    assertThat(TreeStringParser.computeDepth("   +- Relation[a#1] p")).isEqualTo(2);
    assertThat(TreeStringParser.computeDepth(":- Project [a#1]")).isEqualTo(1);
    assertThat(TreeStringParser.computeDepth(":  +- Relation[a#1] p")).isEqualTo(2);
  }

  @Test
  void extractNodeTextStripsPrefix() {
    assertThat(TreeStringParser.extractNodeText("Project [a#1]")).isEqualTo("Project [a#1]");
    assertThat(TreeStringParser.extractNodeText("+- Filter true")).isEqualTo("Filter true");
    assertThat(TreeStringParser.extractNodeText("   +- Relation[a#1] p"))
        .isEqualTo("Relation[a#1] p");
    assertThat(TreeStringParser.extractNodeText(":  +- Relation[a#1] p"))
        .isEqualTo("Relation[a#1] p");
  }

  @Test
  void extractBracketContentHandlesNesting() {
    assertThat(TreeStringParser.extractBracketContent("[a, b, c]", 0)).isEqualTo("a, b, c");
    assertThat(TreeStringParser.extractBracketContent("[a, [b, c], d]", 0))
        .isEqualTo("a, [b, c], d");
    assertThat(TreeStringParser.extractBracketContent(
        "Node [cast(null as struct<a:string,b:int>) AS x#1]", 5))
        .isEqualTo("cast(null as struct<a:string,b:int>) AS x#1");
  }

  @Test
  void parsesSerializeFromObjectWithStaticInvoke() throws IOException {
    String plan =
        "SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, "
            + "StringType, fromString, input[0, MyClass, true].name, true, false) AS name#100, "
            + "staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, "
            + "input[0, MyClass, true].email, true, false) AS email#101]\n"
            + "+- Relation[x#200] parquet\n";

    PlanIR ir = parser.parseString(plan);

    NodeIR serialize = ir.getNode(0);
    assertThat(serialize.getType()).isEqualTo("SerializeFromObject");
    assertThat(serialize.getOutput()).hasSize(2);
    assertThat(serialize.getOutput().get(0).getName()).isEqualTo("name");
    assertThat(serialize.getOutput().get(0).getExprId()).isEqualTo(100);
    assertThat(serialize.getOutput().get(1).getName()).isEqualTo("email");
    assertThat(serialize.getOutput().get(1).getExprId()).isEqualTo(101);
  }

  @Test
  void parsesCastWithStructType() throws IOException {
    String plan =
        "Project [cast(null as struct<a:string,b:int>) AS result#50]\n"
            + "+- Relation[x#1] parquet\n";

    PlanIR ir = parser.parseString(plan);

    NodeIR project = ir.getNode(0);
    assertThat(project.getOutput()).hasSize(1);
    assertThat(project.getOutput().get(0).getName()).isEqualTo("result");
    assertThat(project.getOutput().get(0).getExprId()).isEqualTo(50);
  }

  @Test
  void parsesRepartitionAttributes() throws IOException {
    String plan = "Repartition 61, true\n"
        + "+- Relation[a#1] parquet\n";

    PlanIR ir = parser.parseString(plan);

    NodeIR repartition = ir.getNode(0);
    assertThat(repartition.getAttributes().get("numPartitions")).isEqualTo("61");
    assertThat(repartition.getAttributes().get("shuffle")).isEqualTo("true");
  }

  @Test
  void parsesSubqueryAlias() throws IOException {
    String plan = "SubqueryAlias my_table\n"
        + "+- Relation[a#1] parquet\n";

    PlanIR ir = parser.parseString(plan);

    NodeIR alias = ir.getNode(0);
    assertThat(alias.getAttributes().get("alias")).isEqualTo("my_table");
  }

  @Test
  void parsesDeepNestedPlan() throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("Project [x#1]\n");
    sb.append("+- Filter true\n");
    sb.append("   +- Project [x#1]\n");
    sb.append("      +- Filter true\n");
    sb.append("         +- Relation[x#1] parquet\n");

    PlanIR ir = parser.parseString(sb.toString());

    assertThat(ir.nodeCount()).isEqualTo(5);
    assertThat(ir.maxDepth()).isEqualTo(5);

    // Verify chain: 0 → 1 → 2 → 3 → 4
    assertThat(ir.getNode(0).getChildren()).containsExactly(1);
    assertThat(ir.getNode(1).getChildren()).containsExactly(2);
    assertThat(ir.getNode(2).getChildren()).containsExactly(3);
    assertThat(ir.getNode(3).getChildren()).containsExactly(4);
    assertThat(ir.getNode(4).getChildren()).isEmpty();
  }
}
