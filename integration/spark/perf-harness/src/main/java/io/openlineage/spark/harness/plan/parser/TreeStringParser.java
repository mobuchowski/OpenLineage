/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.parser;

import io.openlineage.spark.harness.plan.ir.NodeIR;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses Spark's {@code treeString()} text output into a {@link PlanIR}.
 *
 * <p>Handles the standard Spark plan text format produced by {@code df.explain()},
 * {@code queryExecution.toString()}, or captured from Spark logs:
 *
 * <pre>
 * == Parsed Logical Plan ==
 * InsertIntoHadoopFsRelationCommand gs://bucket/path, ...
 * +- Repartition 61, true
 *    +- SerializeFromObject [col1#123, col2#456, ...]
 *       +- MapElements &lt;function&gt;, ...
 *          +- Union
 *             :- child1
 *             +- child2
 * </pre>
 *
 * <p>The parser extracts:
 * <ul>
 *   <li>Tree structure (node types, parent-child relationships from indentation)</li>
 *   <li>Output columns from {@code [col#id, ...]} patterns (with truncation handling)</li>
 *   <li>Node-specific attributes (paths, formats, join types, etc.)</li>
 * </ul>
 *
 * <p>Expressions are NOT fully reconstructed — the IR captures structure and schemas,
 * not expression semantics.
 */
public class TreeStringParser {

  private static final Logger log = LoggerFactory.getLogger(TreeStringParser.class);

  // Section headers like "== Parsed Logical Plan =="
  private static final Pattern SECTION_HEADER = Pattern.compile("^==\\s+.+\\s+==$");

  /**
   * Parse a plan from a file path.
   */
  public PlanIR parseFile(Path path) throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(path)) {
      PlanIR ir = parse(reader);
      ir.getMetadata().put("sourceFile", path.toString());
      return ir;
    }
  }

  /**
   * Parse a plan from a string.
   */
  public PlanIR parseString(String text) throws IOException {
    return parse(new BufferedReader(new StringReader(text)));
  }

  /**
   * Parse a plan from a reader.
   */
  public PlanIR parse(Reader reader) throws IOException {
    BufferedReader br =
        reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);

    PlanIR plan = new PlanIR();
    plan.setSource("treestring");

    List<NodeIR> nodes = new ArrayList<>();
    // Stack of (depth, nodeId) — used to determine parent-child relationships
    Deque<int[]> stack = new ArrayDeque<>();
    int nextId = 0;

    String line;
    while ((line = br.readLine()) != null) {
      // Skip empty lines and section headers
      if (line.trim().isEmpty() || SECTION_HEADER.matcher(line.trim()).matches()) {
        continue;
      }

      int depth = computeDepth(line);
      String nodeText = extractNodeText(line);

      if (nodeText.isEmpty()) {
        continue;
      }

      String nodeType = extractNodeType(nodeText);
      if (nodeType.isEmpty()) {
        log.warn("Could not extract node type from line: {}",
            line.substring(0, Math.min(line.length(), 80)));
        continue;
      }

      NodeIR node = new NodeIR(nextId++, nodeType);

      // Parse node-specific content
      parseNodeContent(node, nodeType, nodeText);

      // Build parent-child relationships from the depth stack.
      // Pop all nodes at depth >= current depth — they are siblings or descendants
      // of a different parent.
      while (!stack.isEmpty() && stack.peek()[0] >= depth) {
        stack.pop();
      }
      // The top of the stack is the parent (at depth - 1)
      if (!stack.isEmpty()) {
        int parentId = stack.peek()[1];
        nodes.get(parentId).getChildren().add(node.getId());
      }

      stack.push(new int[] {depth, node.getId()});
      nodes.add(node);
    }

    plan.setNodes(nodes);
    if (!nodes.isEmpty()) {
      plan.setRootNodeId(nodes.get(0).getId());
    }

    int recovered = TruncationRecovery.recover(plan);

    log.info(
        "Parsed plan: {} nodes, depth {}, {} leaf nodes, {} cols recovered/synthesized",
        plan.nodeCount(),
        plan.maxDepth(),
        nodes.stream().filter(n -> n.getChildren().isEmpty()).count(),
        recovered);

    return plan;
  }

  /**
   * Compute the tree depth of a line from its indentation.
   *
   * <p>Spark's treeString format uses 3-character units for indentation:
   * <ul>
   *   <li>{@code +- } or {@code :- } — marks a child node</li>
   *   <li>{@code    } or {@code :  } — continuation from parent levels</li>
   * </ul>
   *
   * <p>The depth equals the position of the first alphanumeric character divided by 3.
   * Root nodes (no prefix) have depth 0.
   */
  static int computeDepth(String line) {
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c != ' ' && c != ':' && c != '+' && c != '-' && c != '|') {
        return i / 3;
      }
    }
    // Line is all whitespace/connectors — shouldn't happen for valid input
    return 0;
  }

  /**
   * Extract the node text by stripping the tree-drawing prefix.
   */
  static String extractNodeText(String line) {
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c != ' ' && c != ':' && c != '+' && c != '-' && c != '|') {
        return line.substring(i);
      }
    }
    return "";
  }

  /**
   * Extract the node type (first word) from the node text.
   */
  static String extractNodeType(String nodeText) {
    // Node type is the first word — ends at space, [ or (
    int end = nodeText.length();
    for (int i = 0; i < nodeText.length(); i++) {
      char c = nodeText.charAt(i);
      if (c == ' ' || c == '[' || c == '(') {
        end = i;
        break;
      }
    }
    return nodeText.substring(0, end);
  }

  /**
   * Parse node-specific content and populate the NodeIR.
   *
   * <p>Uses the Tokenizer → ExpressionParser → ColumnExtractor pipeline for bracket content,
   * and simple string operations for node-specific attributes.
   */
  private void parseNodeContent(NodeIR node, String type, String text) {
    // Extract node-specific attributes using simple string operations
    parseNodeAttributes(node, type, text);

    // Extract columns from bracket content using the expression parser pipeline
    int bracketStart = text.indexOf('[');
    if (bracketStart >= 0) {
      String bracketContent = extractBracketContent(text, bracketStart);
      if (bracketContent != null) {
        parseColumnListWithParser(node, bracketContent);
      }
    }
  }

  /**
   * Parse node-specific attributes using simple string operations.
   */
  private void parseNodeAttributes(NodeIR node, String type, String text) {
    switch (type) {
      case "Relation":
      case "LogicalRelation":
        parseRelationAttributes(node, text);
        break;
      case "Join":
        parseJoinAttributes(node, text);
        break;
      case "Filter":
        parseFilterAttributes(node, text);
        break;
      case "InsertIntoHadoopFsRelationCommand":
        parseInsertHadoopAttributes(node, text);
        break;
      case "Repartition":
        parseRepartitionAttributes(node, text);
        break;
      case "SubqueryAlias":
        parseSubqueryAliasAttributes(node, text);
        break;
      default:
        break;
    }
  }

  private void parseRelationAttributes(NodeIR node, String text) {
    // Relation[cols] type — extract type after closing bracket
    int bracketEnd = findMatchingBracket(text, text.indexOf('['));
    if (bracketEnd >= 0 && bracketEnd + 1 < text.length()) {
      String relationType = text.substring(bracketEnd + 1).trim();
      if (!relationType.isEmpty()) {
        node.getAttributes().put("relationType", relationType);
      }
    }
  }

  private void parseJoinAttributes(NodeIR node, String text) {
    // Join LeftOuter, (condition)
    String rest = text.substring("Join".length()).trim();
    int commaIdx = rest.indexOf(',');
    if (commaIdx >= 0) {
      node.getAttributes().put("joinType", rest.substring(0, commaIdx).trim());
      String condition = rest.substring(commaIdx + 1).trim();
      if (!condition.isEmpty()) {
        node.getAttributes().put("condition", condition);
        parseConditionColumns(node, condition);
      }
    } else if (!rest.isEmpty()) {
      node.getAttributes().put("joinType", rest.trim());
    }
  }

  private void parseFilterAttributes(NodeIR node, String text) {
    // Filter <condition>
    String rest = text.substring("Filter".length()).trim();
    if (!rest.isEmpty()) {
      node.getAttributes().put("condition", rest);
      parseConditionColumns(node, rest);
    }
  }

  /**
   * Parse a condition expression and extract all referenced columns.
   */
  private void parseConditionColumns(NodeIR node, String condition) {
    try {
      Tokenizer tokenizer = new Tokenizer(condition);
      List<Token> tokens = tokenizer.tokenize();
      ExpressionParser exprParser = new ExpressionParser(tokens, condition);
      List<ExprNode> expressions = exprParser.parseExpressionList();
      List<io.openlineage.spark.harness.plan.ir.ColumnIR> cols =
          ColumnExtractor.extractAllRefs(expressions);
      if (!cols.isEmpty()) {
        node.setConditionColumns(cols);
      }
    } catch (Exception e) {
      log.debug("Condition parsing failed for join node {}: {}", node.getId(), e.getMessage());
    }
  }

  private void parseInsertHadoopAttributes(NodeIR node, String text) {
    // InsertIntoHadoopFsRelationCommand path, ...
    String rest = text.substring("InsertIntoHadoopFsRelationCommand".length()).trim();
    int spaceIdx = rest.indexOf(' ');
    if (spaceIdx > 0) {
      node.getAttributes().put("path", rest.substring(0, spaceIdx));
    } else if (!rest.isEmpty()) {
      node.getAttributes().put("path", rest);
    }
    if (text.contains("Overwrite")) {
      node.getAttributes().put("mode", "Overwrite");
    } else if (text.contains("Append")) {
      node.getAttributes().put("mode", "Append");
    }
    if (text.contains("AvroFileFormat")) {
      node.getAttributes().put("format", "avro");
    } else if (text.contains("ParquetFileFormat")) {
      node.getAttributes().put("format", "parquet");
    } else if (text.contains("OrcFileFormat")) {
      node.getAttributes().put("format", "orc");
    }
  }

  private void parseRepartitionAttributes(NodeIR node, String text) {
    // Repartition numPartitions, shuffle
    String rest = text.substring("Repartition".length()).trim();
    String[] parts = rest.split(",\\s*", 2);
    if (parts.length >= 1) {
      node.getAttributes().put("numPartitions", parts[0].trim());
    }
    if (parts.length >= 2) {
      node.getAttributes().put("shuffle", parts[1].trim());
    }
  }

  private void parseSubqueryAliasAttributes(NodeIR node, String text) {
    String rest = text.substring("SubqueryAlias".length()).trim();
    if (!rest.isEmpty()) {
      // Alias is first word
      int spaceIdx = rest.indexOf(' ');
      node.getAttributes().put("alias", spaceIdx > 0 ? rest.substring(0, spaceIdx) : rest);
    }
  }

  /**
   * Parse a column list using the Tokenizer → ExpressionParser → ColumnExtractor pipeline.
   */
  private void parseColumnListWithParser(NodeIR node, String content) {
    try {
      Tokenizer tokenizer = new Tokenizer(content);
      List<Token> tokens = tokenizer.tokenize();
      ExpressionParser exprParser = new ExpressionParser(tokens, content);
      List<ExprNode> expressions = exprParser.parseExpressionList();
      ColumnExtractor.Result result = ColumnExtractor.extract(expressions);

      node.setOutput(result.getColumns());
      if (result.getTruncatedCount() > 0) {
        node.setTruncatedColumns(result.getTruncatedCount());
      }
    } catch (Exception e) {
      log.warn("Expression parsing failed for node {}, falling back: {}",
          node.getType(), e.getMessage());
    }
  }

  /**
   * Extract content between matching square brackets, handling nesting.
   */
  static String extractBracketContent(String text, int openPos) {
    int depth = 0;
    for (int i = openPos; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '[') {
        depth++;
      } else if (c == ']') {
        depth--;
        if (depth == 0) {
          return text.substring(openPos + 1, i);
        }
      }
    }
    // Unmatched bracket — return everything after the opening bracket
    return text.substring(openPos + 1);
  }

  /**
   * Find the position of the matching closing bracket.
   */
  private static int findMatchingBracket(String text, int openPos) {
    if (openPos < 0) {
      return -1;
    }
    int depth = 0;
    for (int i = openPos; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '[') {
        depth++;
      } else if (c == ']') {
        depth--;
        if (depth == 0) {
          return i;
        }
      }
    }
    return -1;
  }
}
