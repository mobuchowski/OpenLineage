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

class ExpressionParserTest {

  private List<ExprNode> parse(String input) {
    Tokenizer tokenizer = new Tokenizer(input);
    List<Token> tokens = tokenizer.tokenize();
    ExpressionParser parser = new ExpressionParser(tokens, input);
    return parser.parseExpressionList();
  }

  private ExprNode parseSingle(String input) {
    List<ExprNode> exprs = parse(input);
    assertThat(exprs).hasSize(1);
    return exprs.get(0);
  }

  @Test
  void parsesAttributeRef() {
    ExprNode expr = parseSingle("col#42");
    assertThat(expr).isInstanceOf(ExprNode.AttributeRef.class);
    ExprNode.AttributeRef ref = (ExprNode.AttributeRef) expr;
    assertThat(ref.getName()).isEqualTo("col");
    assertThat(ref.getExprId()).isEqualTo(42);
    assertThat(ref.isLong()).isFalse();
  }

  @Test
  void parsesAttributeRefWithLong() {
    ExprNode expr = parseSingle("col#42L");
    assertThat(expr).isInstanceOf(ExprNode.AttributeRef.class);
    ExprNode.AttributeRef ref = (ExprNode.AttributeRef) expr;
    assertThat(ref.getName()).isEqualTo("col");
    assertThat(ref.getExprId()).isEqualTo(42);
    assertThat(ref.isLong()).isTrue();
  }

  @Test
  void parsesAlias() {
    ExprNode expr = parseSingle("col#42 AS name#100");
    assertThat(expr).isInstanceOf(ExprNode.AliasExpr.class);
    ExprNode.AliasExpr alias = (ExprNode.AliasExpr) expr;
    assertThat(alias.getName()).isEqualTo("name");
    assertThat(alias.getExprId()).isEqualTo(100);
    assertThat(alias.getChild()).isInstanceOf(ExprNode.AttributeRef.class);
  }

  @Test
  void parsesAliasWithLongSuffix() {
    ExprNode expr = parseSingle("col#42 AS id#100L");
    assertThat(expr).isInstanceOf(ExprNode.AliasExpr.class);
    ExprNode.AliasExpr alias = (ExprNode.AliasExpr) expr;
    assertThat(alias.getName()).isEqualTo("id");
    assertThat(alias.getExprId()).isEqualTo(100);
    assertThat(alias.getTypeSuffix()).isEqualTo("L");
  }

  @Test
  void parsesFunctionCall() {
    ExprNode expr = parseSingle("upper(col#1)");
    assertThat(expr).isInstanceOf(ExprNode.FunctionCall.class);
    ExprNode.FunctionCall fn = (ExprNode.FunctionCall) expr;
    assertThat(fn.getName()).isEqualTo("upper");
    assertThat(fn.getArgs()).hasSize(1);
    assertThat(fn.getArgs().get(0)).isInstanceOf(ExprNode.AttributeRef.class);
  }

  @Test
  void parsesNestedFunctionCalls() {
    ExprNode expr = parseSingle("coalesce(upper(a#1), lower(b#2))");
    assertThat(expr).isInstanceOf(ExprNode.FunctionCall.class);
    ExprNode.FunctionCall fn = (ExprNode.FunctionCall) expr;
    assertThat(fn.getName()).isEqualTo("coalesce");
    assertThat(fn.getArgs()).hasSize(2);
    assertThat(fn.getArgs().get(0)).isInstanceOf(ExprNode.FunctionCall.class);
    assertThat(fn.getArgs().get(1)).isInstanceOf(ExprNode.FunctionCall.class);
  }

  @Test
  void parsesStaticInvoke() {
    ExprNode expr = parseSingle(
        "staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, MyClass, true].name, true, false)");
    assertThat(expr).isInstanceOf(ExprNode.FunctionCall.class);
    ExprNode.FunctionCall fn = (ExprNode.FunctionCall) expr;
    assertThat(fn.getName()).isEqualTo("staticinvoke");
    assertThat(fn.getArgs().get(0)).isInstanceOf(ExprNode.LiteralExpr.class);
    assertThat(((ExprNode.LiteralExpr) fn.getArgs().get(0)).getValue())
        .isEqualTo("org.apache.spark.unsafe.types.UTF8String");
  }

  @Test
  void parsesCast() {
    ExprNode expr = parseSingle("cast(null AS struct<a:string,b:int>)");
    assertThat(expr).isInstanceOf(ExprNode.CastExpr.class);
    ExprNode.CastExpr cast = (ExprNode.CastExpr) expr;
    assertThat(cast.getTargetType().getTypeName()).isEqualTo("struct");
    assertThat(cast.getTargetType().getFields()).hasSize(2);
    assertThat(cast.getTargetType().getFields().get(0).getName()).isEqualTo("a");
    assertThat(cast.getTargetType().getFields().get(1).getName()).isEqualTo("b");
  }

  @Test
  void parsesCaseWhen() {
    ExprNode expr = parseSingle("CASE WHEN a#1 = 1 THEN 'yes' ELSE 'no' END");
    assertThat(expr).isInstanceOf(ExprNode.CaseWhenExpr.class);
    ExprNode.CaseWhenExpr cw = (ExprNode.CaseWhenExpr) expr;
    assertThat(cw.getBranches()).hasSize(1);
    assertThat(cw.getElseValue()).isNotNull();
  }

  @Test
  void parsesIfExpr() {
    ExprNode expr = parseSingle("if(a#1 = 1, true, false)");
    assertThat(expr).isInstanceOf(ExprNode.IfExpr.class);
    ExprNode.IfExpr ifExpr = (ExprNode.IfExpr) expr;
    assertThat(ifExpr.getCondition()).isInstanceOf(ExprNode.BinaryExpr.class);
  }

  @Test
  void parsesInputRef() {
    ExprNode expr = parseSingle("input[0, MyClass, true]");
    assertThat(expr).isInstanceOf(ExprNode.InputRef.class);
    ExprNode.InputRef input = (ExprNode.InputRef) expr;
    assertThat(input.getIndex()).isEqualTo(0);
    assertThat(input.getClassName()).isEqualTo("MyClass");
    assertThat(input.isNullable()).isTrue();
  }

  @Test
  void parsesInputRefWithDottedClassName() {
    ExprNode expr = parseSingle("input[0, com.example.MyClass, false]");
    assertThat(expr).isInstanceOf(ExprNode.InputRef.class);
    ExprNode.InputRef input = (ExprNode.InputRef) expr;
    assertThat(input.getClassName()).isEqualTo("com.example.MyClass");
    assertThat(input.isNullable()).isFalse();
  }

  @Test
  void parsesBinaryComparison() {
    ExprNode expr = parseSingle("a#1 = b#2");
    assertThat(expr).isInstanceOf(ExprNode.BinaryExpr.class);
    ExprNode.BinaryExpr bin = (ExprNode.BinaryExpr) expr;
    assertThat(bin.getOperator()).isEqualTo("=");
  }

  @Test
  void parsesBinaryAndOr() {
    ExprNode expr = parseSingle("a#1 = 1 AND b#2 = 2");
    assertThat(expr).isInstanceOf(ExprNode.BinaryExpr.class);
    ExprNode.BinaryExpr bin = (ExprNode.BinaryExpr) expr;
    assertThat(bin.getOperator()).isEqualTo("AND");
  }

  @Test
  void parsesFieldAccess() {
    ExprNode expr = parseSingle("input[0, MyClass, true].name");
    assertThat(expr).isInstanceOf(ExprNode.FieldAccess.class);
    ExprNode.FieldAccess fa = (ExprNode.FieldAccess) expr;
    assertThat(fa.getFieldName()).isEqualTo("name");
    assertThat(fa.getObject()).isInstanceOf(ExprNode.InputRef.class);
  }

  @Test
  void parsesFieldAccessChain() {
    ExprNode expr = parseSingle("input[0, MyClass, true].inner.name");
    assertThat(expr).isInstanceOf(ExprNode.FieldAccess.class);
    ExprNode.FieldAccess fa = (ExprNode.FieldAccess) expr;
    assertThat(fa.getFieldName()).isEqualTo("name");
    assertThat(fa.getObject()).isInstanceOf(ExprNode.FieldAccess.class);
  }

  @Test
  void parsesDataTypeStruct() {
    String input = "cast(null AS struct<a:string,b:int>)";
    ExprNode expr = parseSingle(input);
    ExprNode.CastExpr cast = (ExprNode.CastExpr) expr;
    DataTypeNode dt = cast.getTargetType();
    assertThat(dt.getTypeName()).isEqualTo("struct");
    assertThat(dt.getFields()).hasSize(2);
    assertThat(dt.getFields().get(0).getName()).isEqualTo("a");
    assertThat(dt.getFields().get(0).getType().getTypeName()).isEqualTo("string");
    assertThat(dt.getFields().get(1).getName()).isEqualTo("b");
    assertThat(dt.getFields().get(1).getType().getTypeName()).isEqualTo("int");
  }

  @Test
  void parsesDataTypeArray() {
    String input = "cast(null AS array<int>)";
    ExprNode expr = parseSingle(input);
    ExprNode.CastExpr cast = (ExprNode.CastExpr) expr;
    DataTypeNode dt = cast.getTargetType();
    assertThat(dt.getTypeName()).isEqualTo("array");
    assertThat(dt.getTypeArgs()).hasSize(1);
    assertThat(dt.getTypeArgs().get(0).getTypeName()).isEqualTo("int");
  }

  @Test
  void parsesDataTypeMap() {
    String input = "cast(null AS map<string,int>)";
    ExprNode expr = parseSingle(input);
    ExprNode.CastExpr cast = (ExprNode.CastExpr) expr;
    DataTypeNode dt = cast.getTargetType();
    assertThat(dt.getTypeName()).isEqualTo("map");
    assertThat(dt.getTypeArgs()).hasSize(2);
  }

  @Test
  void parsesNestedStructType() {
    String input = "cast(null AS struct<a:string,nested:struct<x:int,y:int>>)";
    ExprNode expr = parseSingle(input);
    ExprNode.CastExpr cast = (ExprNode.CastExpr) expr;
    DataTypeNode dt = cast.getTargetType();
    assertThat(dt.getTypeName()).isEqualTo("struct");
    assertThat(dt.getFields()).hasSize(2);
    DataTypeNode nestedType = dt.getFields().get(1).getType();
    assertThat(nestedType.getTypeName()).isEqualTo("struct");
    assertThat(nestedType.getFields()).hasSize(2);
  }

  @Test
  void parsesTruncationMarker() {
    ExprNode expr = parseSingle("... 16 more fields");
    assertThat(expr).isInstanceOf(ExprNode.TruncationMarker.class);
    assertThat(((ExprNode.TruncationMarker) expr).getCount()).isEqualTo(16);
  }

  @Test
  void parsesExpressionListWithColumns() {
    List<ExprNode> exprs = parse("a#1, b#2, c#3L");
    assertThat(exprs).hasSize(3);
    assertThat(exprs.get(0)).isInstanceOf(ExprNode.AttributeRef.class);
    assertThat(exprs.get(1)).isInstanceOf(ExprNode.AttributeRef.class);
    assertThat(exprs.get(2)).isInstanceOf(ExprNode.AttributeRef.class);
    assertThat(((ExprNode.AttributeRef) exprs.get(2)).isLong()).isTrue();
  }

  @Test
  void parsesExpressionListWithAliases() {
    List<ExprNode> exprs = parse(
        "upper(col#1) AS name#100, input[0, MyClass, true].age AS age#101");
    assertThat(exprs).hasSize(2);
    assertThat(exprs.get(0)).isInstanceOf(ExprNode.AliasExpr.class);
    assertThat(exprs.get(1)).isInstanceOf(ExprNode.AliasExpr.class);
    assertThat(((ExprNode.AliasExpr) exprs.get(0)).getName()).isEqualTo("name");
    assertThat(((ExprNode.AliasExpr) exprs.get(1)).getName()).isEqualTo("age");
  }

  @Test
  void parsesExpressionListWithTruncation() {
    List<ExprNode> exprs = parse("a#1, b#2, ... 10 more fields");
    assertThat(exprs).hasSize(3);
    assertThat(exprs.get(2)).isInstanceOf(ExprNode.TruncationMarker.class);
    assertThat(((ExprNode.TruncationMarker) exprs.get(2)).getCount()).isEqualTo(10);
  }

  @Test
  void parsesComplexStaticInvokeWithAlias() {
    String input =
        "staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, "
            + "input[0, MyClass, true].name, true, false) AS name#100";
    ExprNode expr = parseSingle(input);
    assertThat(expr).isInstanceOf(ExprNode.AliasExpr.class);
    ExprNode.AliasExpr alias = (ExprNode.AliasExpr) expr;
    assertThat(alias.getName()).isEqualTo("name");
    assertThat(alias.getExprId()).isEqualTo(100);
    assertThat(alias.getChild()).isInstanceOf(ExprNode.FunctionCall.class);
  }

  @Test
  void parsesIsNull() {
    ExprNode expr = parseSingle("a#1 IS NULL");
    assertThat(expr).isInstanceOf(ExprNode.UnaryExpr.class);
    ExprNode.UnaryExpr unary = (ExprNode.UnaryExpr) expr;
    assertThat(unary.getOperator()).isEqualTo("IS NULL");
  }

  @Test
  void parsesIsNotNull() {
    ExprNode expr = parseSingle("a#1 IS NOT NULL");
    assertThat(expr).isInstanceOf(ExprNode.UnaryExpr.class);
    ExprNode.UnaryExpr unary = (ExprNode.UnaryExpr) expr;
    assertThat(unary.getOperator()).isEqualTo("IS NOT NULL");
  }

  @Test
  void parsesNotExpression() {
    ExprNode expr = parseSingle("NOT a#1");
    assertThat(expr).isInstanceOf(ExprNode.UnaryExpr.class);
    ExprNode.UnaryExpr unary = (ExprNode.UnaryExpr) expr;
    assertThat(unary.getOperator()).isEqualTo("NOT");
  }

  @Test
  void recoverFromParseError() {
    // Malformed expression followed by valid one
    List<ExprNode> exprs = parse(") bad stuff, a#1");
    // Should have two entries: opaque for bad, then attribute ref
    assertThat(exprs).hasSize(2);
    assertThat(exprs.get(0)).isInstanceOf(ExprNode.OpaqueExpr.class);
    assertThat(exprs.get(1)).isInstanceOf(ExprNode.AttributeRef.class);
  }

  @Test
  void parsesEmptyInput() {
    List<ExprNode> exprs = parse("");
    assertThat(exprs).isEmpty();
  }

  @Test
  void parsesInExpression() {
    ExprNode expr = parseSingle("a#1 IN (1, 2, 3)");
    assertThat(expr).isInstanceOf(ExprNode.InExpr.class);
    ExprNode.InExpr inExpr = (ExprNode.InExpr) expr;
    assertThat(inExpr.getList()).hasSize(3);
  }

  @Test
  void parsesInWithDottedIdentifiers() {
    String input = "(service_id#6784 IN (CM.991217.tz_pl,CM.991213.tz_de))";
    List<ExprNode> exprs = parse(input);
    assertThat(exprs).hasSize(1);
    ExprNode expr = exprs.get(0);
    assertThat(expr).isInstanceOf(ExprNode.InExpr.class);
    ExprNode.InExpr in = (ExprNode.InExpr) expr;
    assertThat(in.getList()).hasSize(2);
  }

  @Test
  void parsesUdfFunctionCall() {
    ExprNode expr = parseSingle("UDF:classify_bot(client_ip#6176, user_agent#6167)");
    assertThat(expr).isInstanceOf(ExprNode.FunctionCall.class);
    ExprNode.FunctionCall fn = (ExprNode.FunctionCall) expr;
    assertThat(fn.getName()).isEqualTo("UDF:classify_bot");
    assertThat(fn.getArgs()).hasSize(2);
  }

  @Test
  void parsesArithmeticPrecedence() {
    ExprNode expr = parseSingle("a#1 + b#2 * c#3");
    assertThat(expr).isInstanceOf(ExprNode.BinaryExpr.class);
    ExprNode.BinaryExpr bin = (ExprNode.BinaryExpr) expr;
    assertThat(bin.getOperator()).isEqualTo("+");
    assertThat(bin.getRight()).isInstanceOf(ExprNode.BinaryExpr.class);
    assertThat(((ExprNode.BinaryExpr) bin.getRight()).getOperator()).isEqualTo("*");
  }
}
