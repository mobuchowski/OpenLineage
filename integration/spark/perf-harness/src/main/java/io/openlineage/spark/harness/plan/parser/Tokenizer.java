/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Tokenizer {

  private static final Map<String, TokenType> KEYWORDS = new HashMap<>();

  static {
    KEYWORDS.put("AS", TokenType.AS);
    KEYWORDS.put("as", TokenType.AS);
    KEYWORDS.put("CASE", TokenType.CASE);
    KEYWORDS.put("WHEN", TokenType.WHEN);
    KEYWORDS.put("THEN", TokenType.THEN);
    KEYWORDS.put("ELSE", TokenType.ELSE);
    KEYWORDS.put("END", TokenType.END);
    KEYWORDS.put("IN", TokenType.IN);
    KEYWORDS.put("NOT", TokenType.NOT);
    KEYWORDS.put("AND", TokenType.AND);
    KEYWORDS.put("OR", TokenType.OR);
    KEYWORDS.put("IS", TokenType.IS);
    KEYWORDS.put("NULL", TokenType.NULL_KW);
    KEYWORDS.put("null", TokenType.NULL_KW);
    KEYWORDS.put("TRUE", TokenType.TRUE_KW);
    KEYWORDS.put("true", TokenType.TRUE_KW);
    KEYWORDS.put("FALSE", TokenType.FALSE_KW);
    KEYWORDS.put("false", TokenType.FALSE_KW);
    KEYWORDS.put("cast", TokenType.CAST);
    KEYWORDS.put("CAST", TokenType.CAST);
    KEYWORDS.put("if", TokenType.IF);
    KEYWORDS.put("IF", TokenType.IF);
    KEYWORDS.put("struct", TokenType.STRUCT);
    KEYWORDS.put("array", TokenType.ARRAY);
    KEYWORDS.put("map", TokenType.MAP);
    KEYWORDS.put("class", TokenType.CLASS_KW);
  }

  private final char[] input;
  private int pos;

  public Tokenizer(String input) {
    this.input = input.toCharArray();
    this.pos = 0;
  }

  public List<Token> tokenize() {
    List<Token> tokens = new ArrayList<>();
    while (pos < input.length) {
      skipWhitespace();
      if (pos >= input.length) {
        break;
      }
      Token token = nextToken();
      if (token != null) {
        tokens.add(token);
      }
    }
    tokens.add(new Token(TokenType.EOF, "", pos));
    return tokens;
  }

  private void skipWhitespace() {
    while (pos < input.length && (input[pos] == ' ' || input[pos] == '\t')) {
      pos++;
    }
  }

  private Token nextToken() {
    int start = pos;
    char c = input[pos];

    // Ellipsis: "... N more fields"
    if (c == '.' && pos + 2 < input.length && input[pos + 1] == '.' && input[pos + 2] == '.') {
      pos += 3;
      return new Token(TokenType.ELLIPSIS, "...", start);
    }

    // Single-char tokens
    switch (c) {
      case '(':
        pos++;
        return new Token(TokenType.LPAREN, "(", start);
      case ')':
        pos++;
        return new Token(TokenType.RPAREN, ")", start);
      case '[':
        pos++;
        return new Token(TokenType.LBRACKET, "[", start);
      case ']':
        pos++;
        return new Token(TokenType.RBRACKET, "]", start);
      case ',':
        pos++;
        return new Token(TokenType.COMMA, ",", start);
      case '.':
        pos++;
        return new Token(TokenType.DOT, ".", start);
      case ':':
        pos++;
        return new Token(TokenType.COLON, ":", start);
      case '#':
        pos++;
        return new Token(TokenType.HASH, "#", start);
      case '+':
        pos++;
        return new Token(TokenType.PLUS, "+", start);
      case '-':
        pos++;
        return new Token(TokenType.MINUS, "-", start);
      case '*':
        pos++;
        return new Token(TokenType.STAR, "*", start);
      case '/':
        pos++;
        return new Token(TokenType.SLASH, "/", start);
      case '%':
        pos++;
        return new Token(TokenType.PERCENT, "%", start);
      default:
        break;
    }

    // Multi-char operators
    if (c == '!' && pos + 1 < input.length && input[pos + 1] == '=') {
      pos += 2;
      return new Token(TokenType.NEQ, "!=", start);
    }
    if (c == '<') {
      if (pos + 1 < input.length && input[pos + 1] == '=') {
        pos += 2;
        return new Token(TokenType.LTE, "<=", start);
      }
      if (pos + 1 < input.length && input[pos + 1] == '>') {
        pos += 2;
        return new Token(TokenType.NEQ, "<>", start);
      }
      pos++;
      return new Token(TokenType.LT, "<", start);
    }
    if (c == '>') {
      if (pos + 1 < input.length && input[pos + 1] == '=') {
        pos += 2;
        return new Token(TokenType.GTE, ">=", start);
      }
      pos++;
      return new Token(TokenType.GT, ">", start);
    }
    if (c == '=') {
      pos++;
      return new Token(TokenType.EQ, "=", start);
    }
    if (c == '&' && pos + 1 < input.length && input[pos + 1] == '&') {
      pos += 2;
      return new Token(TokenType.AND_OP, "&&", start);
    }
    if (c == '|' && pos + 1 < input.length && input[pos + 1] == '|') {
      pos += 2;
      return new Token(TokenType.OR_OP, "||", start);
    }

    // String literal
    if (c == '\'') {
      return readStringLiteral(start);
    }

    // Number (digit or negative number)
    if (Character.isDigit(c)) {
      return readNumber(start);
    }

    // Identifier or keyword
    if (isIdentStart(c)) {
      return readIdentifierOrKeyword(start);
    }

    // Skip unrecognized character
    pos++;
    return null;
  }

  private Token readStringLiteral(int start) {
    pos++; // skip opening quote
    StringBuilder sb = new StringBuilder();
    while (pos < input.length && input[pos] != '\'') {
      if (input[pos] == '\\' && pos + 1 < input.length) {
        sb.append(input[pos + 1]);
        pos += 2;
      } else {
        sb.append(input[pos]);
        pos++;
      }
    }
    if (pos < input.length) {
      pos++; // skip closing quote
    }
    return new Token(TokenType.STRING_LITERAL, sb.toString(), start);
  }

  private Token readNumber(int start) {
    while (pos < input.length && Character.isDigit(input[pos])) {
      pos++;
    }
    // Check for decimal
    if (pos < input.length && input[pos] == '.' && pos + 1 < input.length
        && Character.isDigit(input[pos + 1])) {
      pos++;
      while (pos < input.length && Character.isDigit(input[pos])) {
        pos++;
      }
    }
    // Check for L/D suffix
    if (pos < input.length && (input[pos] == 'L' || input[pos] == 'D' || input[pos] == 'd')) {
      pos++;
    }
    return new Token(TokenType.NUMBER, new String(input, start, pos - start), start);
  }

  private Token readIdentifierOrKeyword(int start) {
    while (pos < input.length && isIdentPart(input[pos])) {
      pos++;
    }
    String word = new String(input, start, pos - start);
    TokenType kwType = KEYWORDS.get(word);
    if (kwType != null) {
      return new Token(kwType, word, start);
    }
    return new Token(TokenType.IDENTIFIER, word, start);
  }

  private static boolean isIdentStart(char c) {
    return Character.isLetter(c) || c == '_' || c == '@' || c == '$';
  }

  private static boolean isIdentPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '@' || c == '$';
  }
}
