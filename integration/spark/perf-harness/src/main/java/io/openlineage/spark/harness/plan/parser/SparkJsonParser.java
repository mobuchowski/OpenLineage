/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.spark.harness.plan.ir.ColumnIR;
import io.openlineage.spark.harness.plan.ir.NodeIR;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses Spark's {@code plan.toJSON()} output into a {@link PlanIR}.
 *
 * <p>Spark's JSON format is a flat array of node objects in pre-order traversal.
 * Each node has:
 * <ul>
 *   <li>{@code "class"} — full Java class name</li>
 *   <li>{@code "num-children"} — number of direct children</li>
 *   <li>Constructor parameter fields serialized by {@code TreeNode.jsonFields}</li>
 * </ul>
 *
 * <p>Children are NOT nested — they follow their parent sequentially in the array.
 * To reconstruct the tree, we use the {@code num-children} field and process nodes
 * in order, assigning children based on the count.
 *
 * <p>This format has no truncation — all columns, types, and expression details
 * are preserved (unlike treeString which truncates with "... N more fields").
 */
public class SparkJsonParser {

  private static final Logger log = LoggerFactory.getLogger(SparkJsonParser.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * Parse a plan from a file containing Spark's toJSON() output.
   */
  public PlanIR parseFile(Path path) throws IOException {
    String content = Files.readString(path);
    PlanIR ir = parseString(content);
    ir.getMetadata().put("sourceFile", path.toString());
    return ir;
  }

  /**
   * Parse a plan from a JSON string (the output of plan.toJSON()).
   */
  public PlanIR parseString(String json) throws IOException {
    JsonNode arrayNode = mapper.readTree(json);
    if (!arrayNode.isArray()) {
      throw new IOException("Expected a JSON array (Spark plan.toJSON() format)");
    }

    PlanIR plan = new PlanIR();
    plan.setSource("spark-json");

    List<NodeIR> nodes = new ArrayList<>();
    List<JsonNode> jsonNodes = new ArrayList<>();
    for (JsonNode n : arrayNode) {
      jsonNodes.add(n);
    }

    // First pass: create all NodeIR objects with their types
    for (int i = 0; i < jsonNodes.size(); i++) {
      JsonNode jn = jsonNodes.get(i);
      String className = jn.has("class") ? jn.get("class").asText() : "Unknown";
      String simpleType = simpleClassName(className);

      NodeIR node = new NodeIR(i, simpleType);
      node.setClassName(className);

      int numChildren = jn.has("num-children") ? jn.get("num-children").asInt() : 0;

      // Extract node-specific attributes from JSON fields
      parseJsonFields(node, jn, className);

      nodes.add(node);
    }

    // Second pass: reconstruct parent-child relationships.
    // Spark serializes nodes in pre-order. Each node's children are the next
    // num-children subtrees in the array. We use a recursive descent approach.
    if (!nodes.isEmpty()) {
      assignChildren(jsonNodes, nodes, 0);
    }

    plan.setNodes(nodes);
    if (!nodes.isEmpty()) {
      plan.setRootNodeId(0);
    }

    log.info(
        "Parsed plan: {} nodes, depth {}, {} leaf nodes",
        plan.nodeCount(),
        plan.maxDepth(),
        nodes.stream().filter(n -> n.getChildren().isEmpty()).count());

    return plan;
  }

  /**
   * Recursively assign children by consuming nodes from the pre-order array.
   *
   * @return the next unprocessed index in the array
   */
  private int assignChildren(List<JsonNode> jsonNodes, List<NodeIR> nodes, int index) {
    if (index >= jsonNodes.size()) {
      return index;
    }

    JsonNode jn = jsonNodes.get(index);
    int numChildren = jn.has("num-children") ? jn.get("num-children").asInt() : 0;
    NodeIR node = nodes.get(index);

    int nextIndex = index + 1;
    for (int c = 0; c < numChildren; c++) {
      if (nextIndex >= nodes.size()) {
        break;
      }
      node.getChildren().add(nextIndex);
      nextIndex = assignChildren(jsonNodes, nodes, nextIndex);
    }
    return nextIndex;
  }

  /**
   * Extract attributes and columns from the JSON fields of a node.
   */
  private void parseJsonFields(NodeIR node, JsonNode jn, String className) {
    // Extract output columns if present
    if (jn.has("output")) {
      parseOutputField(node, jn.get("output"));
    }

    // Node-type-specific attribute extraction
    if (className.endsWith("LogicalRelation")) {
      parseLogicalRelationJson(node, jn);
    } else if (className.endsWith(".Join")) {
      if (jn.has("joinType")) {
        JsonNode joinType = jn.get("joinType");
        if (joinType.isObject() && joinType.has("object")) {
          node.getAttributes().put("joinType", simpleClassName(joinType.get("object").asText()));
        } else if (joinType.isTextual()) {
          node.getAttributes().put("joinType", joinType.asText());
        }
      }
    } else if (className.endsWith("InsertIntoHadoopFsRelationCommand")) {
      if (jn.has("outputPath")) {
        node.getAttributes().put("path", jn.get("outputPath").asText());
      }
    } else if (className.endsWith(".Repartition") || className.endsWith(".RepartitionByExpression")) {
      if (jn.has("numPartitions")) {
        node.getAttributes().put("numPartitions", String.valueOf(jn.get("numPartitions").asInt()));
      }
      if (jn.has("shuffle")) {
        node.getAttributes().put("shuffle", String.valueOf(jn.get("shuffle").asBoolean()));
      }
    } else if (className.endsWith(".SubqueryAlias")) {
      if (jn.has("alias")) {
        node.getAttributes().put("alias", jn.get("alias").asText());
      } else if (jn.has("identifier")) {
        node.getAttributes().put("alias", jn.get("identifier").asText());
      }
    }
  }

  /**
   * Parse the "output" field from JSON — typically a list of AttributeReference objects.
   */
  private void parseOutputField(NodeIR node, JsonNode outputNode) {
    if (!outputNode.isArray()) {
      return;
    }
    List<ColumnIR> columns = new ArrayList<>();
    for (JsonNode attrNode : outputNode) {
      if (attrNode.isObject()) {
        String name = attrNode.has("name") ? attrNode.get("name").asText() : "unknown";
        long exprId = 0;
        if (attrNode.has("exprId")) {
          JsonNode exprIdNode = attrNode.get("exprId");
          if (exprIdNode.isObject() && exprIdNode.has("id")) {
            exprId = exprIdNode.get("id").asLong();
          } else if (exprIdNode.isNumber()) {
            exprId = exprIdNode.asLong();
          }
        }
        String dataType = "string";
        if (attrNode.has("dataType")) {
          dataType = extractDataTypeName(attrNode.get("dataType"));
        }
        columns.add(new ColumnIR(name, exprId, dataType));
      }
    }
    node.setOutput(columns);
  }

  /**
   * Parse LogicalRelation-specific JSON fields.
   */
  private void parseLogicalRelationJson(NodeIR node, JsonNode jn) {
    if (jn.has("relation")) {
      JsonNode relNode = jn.get("relation");
      if (relNode.isObject()) {
        if (relNode.has("class")) {
          node.getAttributes().put("relationType", simpleClassName(relNode.get("class").asText()));
        }
        // Extract paths if present (for HadoopFsRelation)
        if (relNode.has("location")) {
          JsonNode location = relNode.get("location");
          if (location.isObject() && location.has("rootPaths")) {
            node.getAttributes().put("rootPaths", location.get("rootPaths").toString());
          }
        }
      } else if (relNode.isTextual()) {
        node.getAttributes().put("relationType", relNode.asText());
      }
    }

    // catalogTable field
    if (jn.has("catalogTable") && !jn.get("catalogTable").isNull()) {
      JsonNode ct = jn.get("catalogTable");
      if (ct.isObject()) {
        if (ct.has("identifier")) {
          node.getAttributes().put("tableName", ct.get("identifier").toString());
        }
      }
    }
  }

  /**
   * Extract a simple data type name from a Spark DataType JSON value.
   */
  private String extractDataTypeName(JsonNode dtNode) {
    if (dtNode.isTextual()) {
      return dtNode.asText();
    }
    if (dtNode.isObject() && dtNode.has("type")) {
      return dtNode.get("type").asText();
    }
    return "string";
  }

  /**
   * Extract the simple class name from a fully qualified class name.
   */
  static String simpleClassName(String fqcn) {
    // Handle Scala objects (ending with $)
    String clean = fqcn.endsWith("$") ? fqcn.substring(0, fqcn.length() - 1) : fqcn;
    int lastDot = clean.lastIndexOf('.');
    return lastDot >= 0 ? clean.substring(lastDot + 1) : clean;
  }
}
