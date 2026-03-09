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
import java.util.List;
import org.junit.jupiter.api.Test;

class ColumnExtractorTest {

  private ColumnExtractor.Result extractFromString(String input) {
    Tokenizer tokenizer = new Tokenizer(input);
    List<Token> tokens = tokenizer.tokenize();
    ExpressionParser parser = new ExpressionParser(tokens, input);
    List<ExprNode> expressions = parser.parseExpressionList();
    return ColumnExtractor.extract(expressions);
  }

  @Test
  void extractsColumnsFromDirectRefs() {
    ColumnExtractor.Result result = extractFromString("a#1, b#2, c#3L");
    assertThat(result.getColumns()).hasSize(3);
    assertThat(result.getColumns().get(0).getName()).isEqualTo("a");
    assertThat(result.getColumns().get(0).getExprId()).isEqualTo(1);
    assertThat(result.getColumns().get(2).getDataType()).isEqualTo("long");
    assertThat(result.getTruncatedCount()).isEqualTo(0);
  }

  @Test
  void extractsColumnsFromAliases() {
    ColumnExtractor.Result result = extractFromString(
        "upper(x#1) AS name#100, input[0, MyClass, true].age AS age#101");
    assertThat(result.getColumns()).hasSize(2);
    assertThat(result.getColumns().get(0).getName()).isEqualTo("name");
    assertThat(result.getColumns().get(0).getExprId()).isEqualTo(100);
    assertThat(result.getColumns().get(1).getName()).isEqualTo("age");
    assertThat(result.getColumns().get(1).getExprId()).isEqualTo(101);
  }

  @Test
  void extractsTruncationCount() {
    ColumnExtractor.Result result = extractFromString("a#1, b#2, ... 16 more fields");
    assertThat(result.getColumns()).hasSize(2);
    assertThat(result.getTruncatedCount()).isEqualTo(16);
  }

  @Test
  void extractsMixedExpressionsAndColumns() {
    ColumnExtractor.Result result = extractFromString(
        "staticinvoke(class UTF8String, StringType, fromString, "
            + "input[0, MyClass, true].name, true, false) AS name#100, "
            + "input[0, MyClass, true].age AS age#101, "
            + "... 10 more fields");
    assertThat(result.getColumns()).hasSize(2);
    assertThat(result.getColumns().get(0).getName()).isEqualTo("name");
    assertThat(result.getColumns().get(1).getName()).isEqualTo("age");
    assertThat(result.getTruncatedCount()).isEqualTo(10);
  }

  @Test
  void extractsFromEmptyInput() {
    ColumnExtractor.Result result = extractFromString("");
    assertThat(result.getColumns()).isEmpty();
    assertThat(result.getTruncatedCount()).isEqualTo(0);
  }

  @Test
  void extractsAllRefsFromCondition() {
    String input = "(a#1.service_id = b#2.service_id) && (a#1.client_id = b#2.client_id)";
    Tokenizer tokenizer = new Tokenizer(input);
    List<Token> tokens = tokenizer.tokenize();
    ExpressionParser parser = new ExpressionParser(tokens, input);
    List<ExprNode> expressions = parser.parseExpressionList();
    List<ColumnIR> refs = ColumnExtractor.extractAllRefs(expressions);
    assertThat(refs).hasSize(2);
    assertThat(refs.stream().anyMatch(c -> c.getName().equals("a") && c.getExprId() == 1)).isTrue();
    assertThat(refs.stream().anyMatch(c -> c.getName().equals("b") && c.getExprId() == 2)).isTrue();
  }

  @Test
  void extractsAllRefsDeduplicates() {
    String input = "a#1 = 1 AND a#1 = 2";
    Tokenizer tokenizer = new Tokenizer(input);
    List<Token> tokens = tokenizer.tokenize();
    ExpressionParser parser = new ExpressionParser(tokens, input);
    List<ExprNode> expressions = parser.parseExpressionList();
    List<ColumnIR> refs = ColumnExtractor.extractAllRefs(expressions);
    assertThat(refs).hasSize(1);
    assertThat(refs.get(0).getName()).isEqualTo("a");
  }

  @Test
  void extractsLongTypeFromAlias() {
    ColumnExtractor.Result result = extractFromString("col#1 AS id#100L");
    assertThat(result.getColumns()).hasSize(1);
    ColumnIR col = result.getColumns().get(0);
    assertThat(col.getName()).isEqualTo("id");
    assertThat(col.getExprId()).isEqualTo(100);
    assertThat(col.getDataType()).isEqualTo("long");
  }
}
