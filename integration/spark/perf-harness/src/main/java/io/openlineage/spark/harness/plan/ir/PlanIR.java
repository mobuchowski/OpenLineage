/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan.ir;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Intermediate representation of a complete Spark LogicalPlan tree.
 *
 * <p>This is the bridge between serialized plan formats (treeString text, Spark JSON) and the
 * harness's LogicalPlan construction. The IR captures plan structure and schemas but intentionally
 * simplifies expressions.
 *
 * <p>Workflow:
 * <ol>
 *   <li>Capture a plan from a real Spark job (via {@code explain()}, logs, or {@code toJSON()})</li>
 *   <li>Parse it into PlanIR using {@code TreeStringParser} or {@code SparkJsonParser}</li>
 *   <li>Optionally save/edit the IR as JSON</li>
 *   <li>Load the IR in the harness via {@code PlanIRLoader} to build real LogicalPlan nodes</li>
 * </ol>
 */
public class PlanIR {
  private String version = "1";
  private String source;
  private int rootNodeId;
  private List<NodeIR> nodes = new ArrayList<>();
  private Map<String, String> metadata = new HashMap<>();

  public PlanIR() {}

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public int getRootNodeId() {
    return rootNodeId;
  }

  public void setRootNodeId(int rootNodeId) {
    this.rootNodeId = rootNodeId;
  }

  public List<NodeIR> getNodes() {
    return nodes;
  }

  public void setNodes(List<NodeIR> nodes) {
    this.nodes = nodes;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public NodeIR getNode(int id) {
    return nodes.stream().filter(n -> n.getId() == id).findFirst().orElse(null);
  }

  /** Total number of nodes in the plan. */
  public int nodeCount() {
    return nodes.size();
  }

  /** Maximum depth of the plan tree. */
  public int maxDepth() {
    return maxDepth(rootNodeId, 0);
  }

  private int maxDepth(int nodeId, int current) {
    NodeIR node = getNode(nodeId);
    if (node == null || node.getChildren().isEmpty()) {
      return current + 1;
    }
    return node.getChildren().stream()
        .mapToInt(childId -> maxDepth(childId, current + 1))
        .max()
        .orElse(current + 1);
  }

  @Override
  public String toString() {
    return "PlanIR{source=" + source + ", nodes=" + nodes.size()
        + ", depth=" + maxDepth() + "}";
  }
}
