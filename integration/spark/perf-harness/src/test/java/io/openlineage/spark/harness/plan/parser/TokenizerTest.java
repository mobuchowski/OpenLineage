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

import java.util.List;
import org.junit.jupiter.api.Test;

class TokenizerTest {

  private List<Token> tokenize(String input) {
    return new Tokenizer(input).tokenize();
  }

  @Test
  void tokenizesIdentifier() {
    List<Token> tokens = tokenize("col_name");
    assertThat(tokens).hasSize(2);
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(0).getValue()).isEqualTo("col_name");
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.EOF);
  }

  @Test
  void tokenizesAttributeRef() {
    List<Token> tokens = tokenize("name#42");
    assertThat(tokens).hasSize(4); // IDENTIFIER, HASH, NUMBER, EOF
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(0).getValue()).isEqualTo("name");
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.HASH);
    assertThat(tokens.get(2).getType()).isEqualTo(TokenType.NUMBER);
    assertThat(tokens.get(2).getValue()).isEqualTo("42");
  }

  @Test
  void tokenizesNumberWithLongSuffix() {
    List<Token> tokens = tokenize("123L");
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.NUMBER);
    assertThat(tokens.get(0).getValue()).isEqualTo("123L");
  }

  @Test
  void tokenizesStringLiteral() {
    List<Token> tokens = tokenize("'hello world'");
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.STRING_LITERAL);
    assertThat(tokens.get(0).getValue()).isEqualTo("hello world");
  }

  @Test
  void tokenizesOperators() {
    List<Token> tokens = tokenize("= != < > <= >=");
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.EQ);
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.NEQ);
    assertThat(tokens.get(2).getType()).isEqualTo(TokenType.LT);
    assertThat(tokens.get(3).getType()).isEqualTo(TokenType.GT);
    assertThat(tokens.get(4).getType()).isEqualTo(TokenType.LTE);
    assertThat(tokens.get(5).getType()).isEqualTo(TokenType.GTE);
  }

  @Test
  void tokenizesKeywords() {
    List<Token> tokens = tokenize("AS CASE WHEN THEN ELSE END cast");
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.AS);
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.CASE);
    assertThat(tokens.get(2).getType()).isEqualTo(TokenType.WHEN);
    assertThat(tokens.get(3).getType()).isEqualTo(TokenType.THEN);
    assertThat(tokens.get(4).getType()).isEqualTo(TokenType.ELSE);
    assertThat(tokens.get(5).getType()).isEqualTo(TokenType.END);
    assertThat(tokens.get(6).getType()).isEqualTo(TokenType.CAST);
  }

  @Test
  void tokenizesEllipsis() {
    List<Token> tokens = tokenize("... 10 more fields");
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.ELLIPSIS);
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.NUMBER);
    assertThat(tokens.get(1).getValue()).isEqualTo("10");
    assertThat(tokens.get(2).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(2).getValue()).isEqualTo("more");
  }

  @Test
  void tokenizesBracketsAndParens() {
    List<Token> tokens = tokenize("foo(a, [b])");
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.LPAREN);
    assertThat(tokens.get(2).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(3).getType()).isEqualTo(TokenType.COMMA);
    assertThat(tokens.get(4).getType()).isEqualTo(TokenType.LBRACKET);
    assertThat(tokens.get(5).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(6).getType()).isEqualTo(TokenType.RBRACKET);
    assertThat(tokens.get(7).getType()).isEqualTo(TokenType.RPAREN);
  }

  @Test
  void tokenizesAngleBracketsAsSeparateTokens() {
    List<Token> tokens = tokenize("struct<a:int,b:string>");
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.STRUCT);
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.LT);
    assertThat(tokens.get(2).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(3).getType()).isEqualTo(TokenType.COLON);
  }

  @Test
  void tokenizesLogicalOperators() {
    List<Token> tokens = tokenize("a && b || c");
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.AND_OP);
    assertThat(tokens.get(3).getType()).isEqualTo(TokenType.OR_OP);
  }

  @Test
  void tokenizesDotSeparately() {
    List<Token> tokens = tokenize("input.name");
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.DOT);
    assertThat(tokens.get(2).getType()).isEqualTo(TokenType.IDENTIFIER);
  }

  @Test
  void tokenizesClassKeyword() {
    List<Token> tokens = tokenize("class org.UTF8String");
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.CLASS_KW);
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(1).getValue()).isEqualTo("org");
    assertThat(tokens.get(2).getType()).isEqualTo(TokenType.DOT);
  }

  @Test
  void tokenizesNestedExpression() {
    List<Token> tokens = tokenize(
        "staticinvoke(class UTF8String, StringType, fromString, input[0, MyClass, true].name)");
    // Should produce tokens without error
    assertThat(tokens).hasSizeGreaterThan(5);
    assertThat(tokens.get(0).getType()).isEqualTo(TokenType.IDENTIFIER);
    assertThat(tokens.get(0).getValue()).isEqualTo("staticinvoke");
  }

  @Test
  void tokenizesArithmeticOperators() {
    List<Token> tokens = tokenize("a + b * c - d / e % f");
    assertThat(tokens.get(1).getType()).isEqualTo(TokenType.PLUS);
    assertThat(tokens.get(3).getType()).isEqualTo(TokenType.STAR);
    assertThat(tokens.get(5).getType()).isEqualTo(TokenType.MINUS);
    assertThat(tokens.get(7).getType()).isEqualTo(TokenType.SLASH);
    assertThat(tokens.get(9).getType()).isEqualTo(TokenType.PERCENT);
  }
}
