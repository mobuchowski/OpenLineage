/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.parser;

public class Token {
  private final TokenType type;
  private final String value;
  private final int position;

  public Token(TokenType type, String value, int position) {
    this.type = type;
    this.value = value;
    this.position = position;
  }

  public TokenType getType() {
    return type;
  }

  public String getValue() {
    return value;
  }

  public int getPosition() {
    return position;
  }

  @Override
  public String toString() {
    return type + "(" + value + ")@" + position;
  }
}
