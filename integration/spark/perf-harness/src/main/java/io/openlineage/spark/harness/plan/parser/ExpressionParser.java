/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.parser;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recursive descent parser for Spark treeString expression syntax.
 *
 * <p>Parses the comma-separated expression lists found inside square brackets of Spark plan nodes.
 * Handles: attribute references (name#id), aliases (expr AS name#id), function calls,
 * staticinvoke, cast, CASE/WHEN, if, input references, binary/unary operators, field access,
 * struct/array/map types, and truncation markers.
 *
 * <p>Grammar is LL(2). Error recovery skips to next top-level comma on parse failure.
 */
public class ExpressionParser {

  private static final Logger log = LoggerFactory.getLogger(ExpressionParser.class);

  private final List<Token> tokens;
  private int pos;
  private final String rawInput;

  public ExpressionParser(List<Token> tokens, String rawInput) {
    this.tokens = tokens;
    this.pos = 0;
    this.rawInput = rawInput;
  }

  public ExpressionParser(List<Token> tokens) {
    this(tokens, "");
  }

  private Token peek() {
    if (pos >= tokens.size()) {
      return new Token(TokenType.EOF, "", -1);
    }
    return tokens.get(pos);
  }

  private Token peek(int offset) {
    int idx = pos + offset;
    if (idx >= tokens.size()) {
      return new Token(TokenType.EOF, "", -1);
    }
    return tokens.get(idx);
  }

  private Token advance() {
    Token t = peek();
    pos++;
    return t;
  }

  private boolean check(TokenType type) {
    return peek().getType() == type;
  }

  private boolean match(TokenType type) {
    if (check(type)) {
      advance();
      return true;
    }
    return false;
  }

  private Token expect(TokenType type) {
    if (check(type)) {
      return advance();
    }
    throw new ParseException(
        "Expected " + type + " but got " + peek().getType() + "(" + peek().getValue() + ")"
            + " at position " + peek().getPosition());
  }

  /**
   * Parse a top-level comma-separated expression list.
   */
  public List<ExprNode> parseExpressionList() {
    List<ExprNode> result = new ArrayList<>();
    if (check(TokenType.EOF)) {
      return result;
    }

    while (!check(TokenType.EOF)) {
      int startPos = pos;
      try {
        ExprNode expr = parseExpression();
        result.add(expr);
      } catch (ParseException e) {
        log.debug("Parse error at position {}: {}", pos, e.getMessage());
        String skipped = skipToNextComma();
        result.add(new ExprNode.OpaqueExpr(skipped));
      }

      if (!match(TokenType.COMMA)) {
        break;
      }
    }
    return result;
  }

  /**
   * Parse a single expression, including AS alias and IN postfix.
   */
  public ExprNode parseExpression() {
    // Check for truncation marker: ... N more fields
    if (check(TokenType.ELLIPSIS)) {
      return parseTruncationMarker();
    }

    ExprNode expr = parseBinaryOr();

    // Check for AS alias: expr AS name#id
    if (check(TokenType.AS)) {
      advance();
      Token name = expect(TokenType.IDENTIFIER);
      String aliasName = name.getValue();
      long exprId = -1;
      String typeSuffix = "";
      if (check(TokenType.HASH)) {
        advance();
        Token idToken = expect(TokenType.NUMBER);
        String idStr = idToken.getValue();
        if (idStr.endsWith("L")) {
          typeSuffix = "L";
          idStr = idStr.substring(0, idStr.length() - 1);
        }
        exprId = Long.parseLong(idStr);
      }
      return new ExprNode.AliasExpr(expr, aliasName, exprId, typeSuffix);
    }

    return expr;
  }

  private ExprNode parseBinaryOr() {
    ExprNode left = parseBinaryAnd();
    while (check(TokenType.OR_OP) || check(TokenType.OR)) {
      String op = advance().getValue();
      ExprNode right = parseBinaryAnd();
      left = new ExprNode.BinaryExpr(left, op, right);
    }
    return left;
  }

  private ExprNode parseBinaryAnd() {
    ExprNode left = parseComparison();
    while (check(TokenType.AND_OP) || check(TokenType.AND)) {
      String op = advance().getValue();
      ExprNode right = parseComparison();
      left = new ExprNode.BinaryExpr(left, op, right);
    }
    return left;
  }

  private ExprNode parseComparison() {
    ExprNode left = parseAddSub();

    if (check(TokenType.EQ) || check(TokenType.NEQ) || check(TokenType.LT)
        || check(TokenType.GT) || check(TokenType.LTE) || check(TokenType.GTE)) {
      String op = advance().getValue();
      ExprNode right = parseAddSub();
      left = new ExprNode.BinaryExpr(left, op, right);
    }

    // IN (list)
    if (check(TokenType.IN)) {
      advance();
      expect(TokenType.LPAREN);
      List<ExprNode> list = new ArrayList<>();
      if (!check(TokenType.RPAREN)) {
        list.add(parseExpression());
        while (match(TokenType.COMMA)) {
          if (check(TokenType.RPAREN)) {
            break;
          }
          list.add(parseExpression());
        }
      }
      expect(TokenType.RPAREN);
      left = new ExprNode.InExpr(left, list);
    }

    // IS NULL / IS NOT NULL
    if (check(TokenType.IS)) {
      advance();
      if (check(TokenType.NOT)) {
        advance();
        expect(TokenType.NULL_KW);
        left = new ExprNode.UnaryExpr("IS NOT NULL", left);
      } else {
        expect(TokenType.NULL_KW);
        left = new ExprNode.UnaryExpr("IS NULL", left);
      }
    }

    return left;
  }

  private ExprNode parseAddSub() {
    ExprNode left = parseMulDiv();
    while (check(TokenType.PLUS) || check(TokenType.MINUS)) {
      String op = advance().getValue();
      ExprNode right = parseMulDiv();
      left = new ExprNode.BinaryExpr(left, op, right);
    }
    return left;
  }

  private ExprNode parseMulDiv() {
    ExprNode left = parseUnary();
    while (check(TokenType.STAR) || check(TokenType.SLASH) || check(TokenType.PERCENT)) {
      String op = advance().getValue();
      ExprNode right = parseUnary();
      left = new ExprNode.BinaryExpr(left, op, right);
    }
    return left;
  }

  private ExprNode parseUnary() {
    if (check(TokenType.NOT)) {
      advance();
      ExprNode child = parseUnary();
      return new ExprNode.UnaryExpr("NOT", child);
    }
    if (check(TokenType.MINUS)) {
      advance();
      ExprNode child = parseUnary();
      return new ExprNode.UnaryExpr("-", child);
    }
    ExprNode expr = parsePrimary();
    return parsePostfix(expr);
  }

  private ExprNode parsePrimary() {
    // NULL literal
    if (check(TokenType.NULL_KW)) {
      Token t = advance();
      return new ExprNode.LiteralExpr(t.getValue(), "null");
    }

    // Boolean literal
    if (check(TokenType.TRUE_KW) || check(TokenType.FALSE_KW)) {
      Token t = advance();
      return new ExprNode.LiteralExpr(t.getValue(), "boolean");
    }

    // Number literal
    if (check(TokenType.NUMBER)) {
      Token t = advance();
      return new ExprNode.LiteralExpr(t.getValue(), "number");
    }

    // String literal
    if (check(TokenType.STRING_LITERAL)) {
      Token t = advance();
      return new ExprNode.LiteralExpr(t.getValue(), "string");
    }

    // CAST(expr AS type)
    if (check(TokenType.CAST)) {
      return parseCast();
    }

    // CASE WHEN ... THEN ... ELSE ... END
    if (check(TokenType.CASE)) {
      return parseCaseWhen();
    }

    // IF(cond, trueVal, falseVal)
    if (check(TokenType.IF)) {
      return parseIf();
    }

    // Parenthesized expression
    if (check(TokenType.LPAREN)) {
      advance();
      ExprNode inner = parseExpression();
      expect(TokenType.RPAREN);
      return inner;
    }

    // map() / array() / struct() as function calls in expression context
    if ((check(TokenType.MAP) || check(TokenType.ARRAY) || check(TokenType.STRUCT))
        && peek(1).getType() == TokenType.LPAREN) {
      Token kw = advance();
      expect(TokenType.LPAREN);
      List<ExprNode> args = new ArrayList<>();
      if (!check(TokenType.RPAREN)) {
        args.add(parseExpression());
        while (match(TokenType.COMMA)) {
          if (check(TokenType.RPAREN)) {
            break;
          }
          args.add(parseExpression());
        }
      }
      expect(TokenType.RPAREN);
      return new ExprNode.FunctionCall(kw.getValue(), args);
    }

    // Identifier-based: attribute ref, function call, input ref, staticinvoke, etc.
    if (check(TokenType.IDENTIFIER)) {
      return parseIdentifierBased();
    }

    throw new ParseException(
        "Unexpected token: " + peek().getType() + "(" + peek().getValue() + ")"
            + " at position " + peek().getPosition());
  }

  private ExprNode parseIdentifierBased() {
    String name = peek().getValue();

    // input[idx, ClassName, nullable]
    if (name.equals("input") && peek(1).getType() == TokenType.LBRACKET) {
      return parseInputRef();
    }

    // staticinvoke(class com.pkg.Class, Type, method, args...)
    if (name.equals("staticinvoke") && peek(1).getType() == TokenType.LPAREN) {
      return parseStaticInvoke();
    }

    // lambdafunction, named_struct, etc. — general function call
    if (peek(1).getType() == TokenType.LPAREN) {
      return parseFunctionCall();
    }

    // UDF:funcName(...) — colon-qualified function call
    if (peek(1).getType() == TokenType.COLON
        && peek(2).getType() == TokenType.IDENTIFIER
        && peek(3).getType() == TokenType.LPAREN) {
      Token prefix = advance(); // UDF
      advance(); // colon
      // Now peek is funcName and peek(1) is LPAREN — parse as function call
      return parseFunctionCall(prefix.getValue() + ":");
    }

    // Identifier followed by # → attribute reference
    if (peek(1).getType() == TokenType.HASH) {
      return parseAttributeRef();
    }

    // Plain identifier (e.g., function without parens in some contexts)
    Token t = advance();
    return new ExprNode.LiteralExpr(t.getValue(), "identifier");
  }

  private ExprNode parseAttributeRef() {
    Token name = advance(); // identifier
    expect(TokenType.HASH);
    Token idToken = expect(TokenType.NUMBER);
    String idStr = idToken.getValue();
    boolean isLong = false;
    if (idStr.endsWith("L")) {
      isLong = true;
      idStr = idStr.substring(0, idStr.length() - 1);
    }
    long exprId = Long.parseLong(idStr);
    return new ExprNode.AttributeRef(name.getValue(), exprId, isLong);
  }

  private ExprNode parseFunctionCall() {
    return parseFunctionCall("");
  }

  private ExprNode parseFunctionCall(String prefix) {
    Token name = advance(); // function name
    expect(TokenType.LPAREN);
    List<ExprNode> args = new ArrayList<>();
    if (!check(TokenType.RPAREN)) {
      args.add(parseExpression());
      while (match(TokenType.COMMA)) {
        if (check(TokenType.RPAREN)) {
          break;
        }
        args.add(parseExpression());
      }
    }
    expect(TokenType.RPAREN);
    return new ExprNode.FunctionCall(prefix + name.getValue(), args);
  }

  private ExprNode parseStaticInvoke() {
    advance(); // "staticinvoke"
    expect(TokenType.LPAREN);

    // Parse: class com.pkg.ClassName
    String className = "";
    if (check(TokenType.CLASS_KW)) {
      advance();
      className = parseDottedName();
    }

    List<ExprNode> args = new ArrayList<>();
    // After class name, consume remaining comma-separated args
    while (match(TokenType.COMMA)) {
      if (check(TokenType.RPAREN)) {
        break;
      }
      args.add(parseExpression());
    }
    expect(TokenType.RPAREN);

    List<ExprNode> fullArgs = new ArrayList<>();
    fullArgs.add(new ExprNode.LiteralExpr(className, "class"));
    fullArgs.addAll(args);
    return new ExprNode.FunctionCall("staticinvoke", fullArgs);
  }

  private String parseDottedName() {
    StringBuilder sb = new StringBuilder();
    if (check(TokenType.IDENTIFIER)) {
      sb.append(advance().getValue());
      while (check(TokenType.DOT) && peek(1).getType() == TokenType.IDENTIFIER) {
        advance(); // dot
        sb.append(".").append(advance().getValue());
      }
    }
    return sb.toString();
  }

  private ExprNode parseInputRef() {
    advance(); // "input"
    expect(TokenType.LBRACKET);
    Token idx = expect(TokenType.NUMBER);
    expect(TokenType.COMMA);

    // Class name: may be dotted
    String className = parseDottedName();

    expect(TokenType.COMMA);
    Token nullable = advance();
    expect(TokenType.RBRACKET);

    return new ExprNode.InputRef(
        Integer.parseInt(idx.getValue()),
        className,
        "true".equals(nullable.getValue()));
  }

  private ExprNode parseCast() {
    advance(); // CAST
    expect(TokenType.LPAREN);
    // Use parseBinaryOr (not parseExpression) to avoid consuming AS as alias
    ExprNode child = parseBinaryOr();
    expect(TokenType.AS);
    DataTypeNode type = parseDataType();
    expect(TokenType.RPAREN);
    return new ExprNode.CastExpr(child, type);
  }

  private ExprNode parseCaseWhen() {
    advance(); // CASE
    List<ExprNode[]> branches = new ArrayList<>();
    ExprNode elseValue = null;

    while (check(TokenType.WHEN)) {
      advance(); // WHEN
      ExprNode condition = parseExpression();
      expect(TokenType.THEN);
      ExprNode result = parseExpression();
      branches.add(new ExprNode[] {condition, result});
    }

    if (match(TokenType.ELSE)) {
      elseValue = parseExpression();
    }

    expect(TokenType.END);
    return new ExprNode.CaseWhenExpr(branches, elseValue);
  }

  private ExprNode parseIf() {
    advance(); // IF
    expect(TokenType.LPAREN);
    ExprNode condition = parseExpression();
    expect(TokenType.COMMA);
    ExprNode trueVal = parseExpression();
    expect(TokenType.COMMA);
    ExprNode falseVal = parseExpression();
    expect(TokenType.RPAREN);
    return new ExprNode.IfExpr(condition, trueVal, falseVal);
  }

  private ExprNode parsePostfix(ExprNode expr) {
    // Field access: expr.fieldName or expr.123 (for dotted identifiers like CM.991217.tz_pl)
    while (check(TokenType.DOT)) {
      if (peek(1).getType() == TokenType.IDENTIFIER) {
        advance(); // dot
        Token field = advance();
        expr = new ExprNode.FieldAccess(expr, field.getValue());
      } else if (peek(1).getType() == TokenType.NUMBER) {
        advance(); // dot
        Token num = advance();
        expr = new ExprNode.FieldAccess(expr, num.getValue());
      } else {
        break;
      }
    }
    return expr;
  }

  private ExprNode parseTruncationMarker() {
    expect(TokenType.ELLIPSIS);
    Token num = expect(TokenType.NUMBER);
    // Skip "more" and "fields" as identifiers
    if (check(TokenType.IDENTIFIER) && "more".equals(peek().getValue())) {
      advance();
    }
    if (check(TokenType.IDENTIFIER) && "fields".equals(peek().getValue())) {
      advance();
    }
    return new ExprNode.TruncationMarker(Integer.parseInt(num.getValue()));
  }

  /**
   * Parse a data type, including parameterized types like struct, array, map.
   */
  public DataTypeNode parseDataType() {
    String typeName;

    if (check(TokenType.STRUCT)) {
      advance();
      return parseStructType(false);
    }
    if (check(TokenType.ARRAY)) {
      advance();
      return parseArrayType();
    }
    if (check(TokenType.MAP)) {
      advance();
      return parseMapType();
    }

    // Simple type name (may be multi-word like "decimal(10,2)")
    if (check(TokenType.IDENTIFIER) || check(TokenType.NULL_KW)) {
      Token t = advance();
      typeName = t.getValue();
    } else {
      throw new ParseException(
          "Expected type name but got " + peek().getType() + " at position " + peek().getPosition());
    }

    // Check for precision/scale like decimal(10, 2)
    if (check(TokenType.LPAREN)) {
      advance();
      StringBuilder params = new StringBuilder(typeName).append("(");
      int depth = 1;
      while (depth > 0 && !check(TokenType.EOF)) {
        Token t = advance();
        if (t.getType() == TokenType.LPAREN) {
          depth++;
        } else if (t.getType() == TokenType.RPAREN) {
          depth--;
          if (depth == 0) {
            break;
          }
        }
        params.append(t.getValue());
      }
      params.append(")");
      typeName = params.toString();
    }

    return new DataTypeNode(typeName);
  }

  private DataTypeNode parseStructType(boolean alreadyOpenAngle) {
    List<DataTypeNode.FieldDef> fields = new ArrayList<>();
    if (!alreadyOpenAngle) {
      if (!check(TokenType.LT)) {
        return new DataTypeNode("struct");
      }
      advance(); // <
    }

    while (!check(TokenType.GT) && !check(TokenType.EOF)) {
      // Truncation inside struct: ... N more fields
      if (check(TokenType.ELLIPSIS)) {
        advance();
        // just skip past "N more fields"
        if (check(TokenType.NUMBER)) {
          advance();
        }
        if (check(TokenType.IDENTIFIER)) {
          advance(); // "more"
        }
        if (check(TokenType.IDENTIFIER)) {
          advance(); // "fields"
        }
        break;
      }

      if (!check(TokenType.IDENTIFIER)) {
        break;
      }
      Token fieldName = advance();
      expect(TokenType.COLON);
      DataTypeNode fieldType = parseDataType();
      fields.add(new DataTypeNode.FieldDef(fieldName.getValue(), fieldType));

      if (!match(TokenType.COMMA)) {
        break;
      }
    }

    if (check(TokenType.GT)) {
      advance();
    }

    return new DataTypeNode("struct", new ArrayList<DataTypeNode>(), fields);
  }

  private DataTypeNode parseArrayType() {
    if (!check(TokenType.LT)) {
      return new DataTypeNode("array");
    }
    advance(); // <
    DataTypeNode elementType = parseDataType();
    if (check(TokenType.GT)) {
      advance();
    }
    List<DataTypeNode> args = new ArrayList<>();
    args.add(elementType);
    return new DataTypeNode("array", args, new ArrayList<DataTypeNode.FieldDef>());
  }

  private DataTypeNode parseMapType() {
    if (!check(TokenType.LT)) {
      return new DataTypeNode("map");
    }
    advance(); // <
    DataTypeNode keyType = parseDataType();
    expect(TokenType.COMMA);
    DataTypeNode valueType = parseDataType();
    if (check(TokenType.GT)) {
      advance();
    }
    List<DataTypeNode> args = new ArrayList<>();
    args.add(keyType);
    args.add(valueType);
    return new DataTypeNode("map", args, new ArrayList<DataTypeNode.FieldDef>());
  }

  /**
   * Skip tokens until the next top-level comma or EOF, returning the skipped text.
   * Always advances at least one token to ensure progress.
   */
  private String skipToNextComma() {
    int startPos = pos < tokens.size() ? tokens.get(pos).getPosition() : 0;
    int depth = 0;
    boolean advanced = false;
    while (!check(TokenType.EOF)) {
      if (check(TokenType.COMMA) && depth <= 0 && advanced) {
        break;
      }
      if (check(TokenType.LPAREN) || check(TokenType.LBRACKET)) {
        depth++;
      } else if (check(TokenType.RPAREN) || check(TokenType.RBRACKET)) {
        depth--;
      }
      advance();
      advanced = true;
      // If depth went negative, we're outside the current expression
      if (depth < 0) {
        depth = 0;
      }
    }
    int endPos = pos < tokens.size() ? tokens.get(pos).getPosition() : rawInput.length();
    if (startPos < endPos && startPos < rawInput.length()) {
      return rawInput.substring(startPos, Math.min(endPos, rawInput.length())).trim();
    }
    return "<unparsable>";
  }

  public static class ParseException extends RuntimeException {
    public ParseException(String message) {
      super(message);
    }
  }
}
