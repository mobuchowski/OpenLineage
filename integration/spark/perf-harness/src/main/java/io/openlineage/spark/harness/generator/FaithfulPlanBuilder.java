/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.generator;

import io.openlineage.spark.harness.plan.ir.ColumnIR;
import io.openlineage.spark.harness.plan.ir.NodeIR;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import io.openlineage.spark.harness.synthetic.SyntheticFileIndex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Alias$;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.Cross$;
import org.apache.spark.sql.catalyst.plans.FullOuter$;
import org.apache.spark.sql.catalyst.plans.Inner$;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.LeftAnti$;
import org.apache.spark.sql.catalyst.plans.LeftOuter$;
import org.apache.spark.sql.catalyst.plans.LeftSemi$;
import org.apache.spark.sql.catalyst.plans.RightOuter$;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate$;
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate$;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.JoinHint;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Repartition;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias$;
import org.apache.spark.sql.catalyst.plans.logical.Union$;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

/**
 * Builds a Spark LogicalPlan that faithfully mirrors the topology of a {@link PlanIR}.
 *
 * <p>Unlike {@link PlanIRLoader} (which builds a generic binary tree), this builder reconstructs
 * the actual node types, parent-child relationships, and per-node column schemas from the IR. The
 * resulting plan exercises OL visitors with the same plan shape as the original production pipeline.
 */
public class FaithfulPlanBuilder {

  private static final SyntheticFileIndex SHARED_FILE_INDEX =
      new SyntheticFileIndex(100, "hdfs://synthetic-cluster/data");

  private FaithfulPlanBuilder() {}

  public static LogicalPlan build(SparkSession spark, PlanIR planIR) {
    List<NodeIR> nodes = planIR.getNodes();
    if (nodes.isEmpty()) {
      throw new IllegalArgumentException("PlanIR has no nodes");
    }

    Map<Integer, NodeIR> byId = new HashMap<>();
    for (NodeIR n : nodes) {
      byId.put(n.getId(), n);
    }
    return buildNode(byId.get(planIR.getRootNodeId()), byId, spark);
  }

  @SuppressWarnings("unchecked")
  private static LogicalPlan buildNode(
      NodeIR node, Map<Integer, NodeIR> byId, SparkSession spark) {
    List<LogicalPlan> children = new ArrayList<>();
    for (int childId : node.getChildren()) {
      children.add(buildNode(byId.get(childId), byId, spark));
    }

    String nodeType = node.getType();
    switch (nodeType) {
      case "Relation":
      case "LogicalRelation":
      case "HiveTableRelation":
        return buildRelation(node, spark);

      case "Project":
        return buildProject(node, singleChild(children, node));

      case "Filter":
      case "TypedFilter":
        return buildFilter(node, singleChild(children, node));

      case "Join":
        if (children.size() >= 2) {
          return buildJoin(node, children.get(0), children.get(1));
        } else if (children.size() == 1) {
          return children.get(0);
        } else {
          return buildFallbackLeaf(node, spark);
        }

      case "Union":
        if (!children.isEmpty()) {
          return buildUnion(children);
        }
        return buildFallbackLeaf(node, spark);

      case "Aggregate":
        return buildAggregate(node, singleChild(children, node));

      case "Repartition":
        return buildRepartition(node, singleChild(children, node));

      case "SubqueryAlias":
        return buildSubqueryAlias(node, singleChild(children, node));

      case "Sort":
        LogicalPlan sortChild = singleChild(children, node);
        return new Project(outputAsNamedSeq(sortChild), sortChild);

      case "Deduplicate":
        LogicalPlan dedupChild = singleChild(children, node);
        return Deduplicate$.MODULE$.apply(
            (Seq<Attribute>) (Seq<?>) outputAsNamedSeq(dedupChild), dedupChild);

      default:
        if (children.size() == 1) {
          LogicalPlan child = children.get(0);
          List<AttributeReference> output = nodeOutput(node);
          if (!output.isEmpty()) {
            List<NamedExpression> aliases = mapOutputAliases(output, child);
            if (aliases.size() == child.output().size()) {
              return new Project(toScalaSeq(aliases), child);
            }
          }
          return new Project(outputAsNamedSeq(child), child);
        } else if (children.size() > 1) {
          // Unknown multi-child node: use first child (Union requires compatible schemas
          // which unknown node types can't guarantee)
          return new Project(outputAsNamedSeq(children.get(0)), children.get(0));
        } else {
          return buildFallbackLeaf(node, spark);
        }
    }
  }

  // ── Node builders ─────────────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private static LogicalRelation buildRelation(NodeIR node, SparkSession spark) {
    StructType schema = nodeSchema(node);
    List<AttributeReference> output = nodeOutput(node);
    Seq<AttributeReference> outputSeq =
        output.isEmpty() ? schemaToOutput(schema) : toScalaSeq(output);

    HadoopFsRelation relation =
        new HadoopFsRelation(
            SHARED_FILE_INDEX,
            new StructType(new StructField[0]),
            schema,
            Option.empty(),
            new ParquetFileFormat(),
            scala.collection.immutable.Map$.MODULE$.<String, String>empty(),
            spark);

    return new LogicalRelation(relation, outputSeq, Option.empty(), false, Option.empty());
  }

  private static Project buildProject(NodeIR node, LogicalPlan child) {
    List<AttributeReference> output = nodeOutput(node);
    if (!output.isEmpty()) {
      List<NamedExpression> projections = mapOutputToChild(output, child);
      return new Project(toScalaSeq(projections), child);
    }
    return new Project(outputAsNamedSeq(child), child);
  }

  private static Filter buildFilter(NodeIR node, LogicalPlan child) {
    Expression condition = buildCondition(node, child);
    return new Filter(condition, child);
  }

  private static LogicalPlan buildJoin(
      NodeIR node, LogicalPlan left, LogicalPlan right) {
    String joinTypeStr = node.getAttributes().getOrDefault("joinType", "Inner");
    JoinType joinType;
    switch (joinTypeStr) {
      case "LeftOuter":
      case "Left":
        joinType = LeftOuter$.MODULE$;
        break;
      case "RightOuter":
      case "Right":
        joinType = RightOuter$.MODULE$;
        break;
      case "FullOuter":
      case "Full":
        joinType = FullOuter$.MODULE$;
        break;
      case "LeftSemi":
        joinType = LeftSemi$.MODULE$;
        break;
      case "LeftAnti":
        joinType = LeftAnti$.MODULE$;
        break;
      case "Cross":
        joinType = Cross$.MODULE$;
        break;
      default:
        joinType = Inner$.MODULE$;
        break;
    }

    Option<Expression> conditionExpr = buildJoinCondition(node, left, right);
    Join joined = new Join(left, right, joinType, conditionExpr, JoinHint.NONE());

    List<AttributeReference> output = nodeOutput(node);
    if (!output.isEmpty()) {
      List<NamedExpression> projections = mapOutputToChild(output, joined);
      return new Project(toScalaSeq(projections), joined);
    }
    return new Project(outputAsNamedSeq(left), joined);
  }

  @SuppressWarnings("unchecked")
  private static LogicalPlan buildUnion(List<LogicalPlan> children) {
    // Union requires all children to have matching schema (same number of columns, compatible types).
    // Normalize all children to match the first child's schema by projecting with matching names/types.
    LogicalPlan first = children.get(0);
    Seq<Attribute> targetSchema = first.output();
    int targetWidth = targetSchema.size();

    List<LogicalPlan> normalized = new ArrayList<>();
    normalized.add(first);

    for (int c = 1; c < children.size(); c++) {
      LogicalPlan child = children.get(c);
      // Project this child to match the target schema width and names
      List<NamedExpression> projections = new ArrayList<>();
      int childWidth = child.output().size();
      for (int i = 0; i < targetWidth; i++) {
        Attribute targetAttr = (Attribute) targetSchema.apply(i);
        if (i < childWidth) {
          // Use child's column but alias to target name/type
          projections.add(newAttr(targetAttr.name(), targetAttr.dataType()));
        } else {
          projections.add(newAttr(targetAttr.name(), targetAttr.dataType()));
        }
      }
      normalized.add(new Project(toScalaSeq(projections), child));
    }
    Seq<LogicalPlan> scalaChildren = toScalaSeq(normalized);
    return Union$.MODULE$.apply(scalaChildren, Union$.MODULE$.apply$default$2(),
        Union$.MODULE$.apply$default$3());
  }

  @SuppressWarnings("unchecked")
  private static LogicalPlan buildAggregate(NodeIR node, LogicalPlan child) {
    List<Expression> groupExprs = new ArrayList<>();
    if (child.output().size() > 0) {
      groupExprs.add((Expression) child.output().apply(0));
    }
    Seq<Expression> groupSeq = toScalaSeq(groupExprs);

    List<AttributeReference> output = nodeOutput(node);
    if (!output.isEmpty()) {
      List<NamedExpression> aggExprs = new ArrayList<>();
      for (AttributeReference a : output) {
        aggExprs.add(a);
      }
      return Aggregate$.MODULE$.apply(
          groupSeq, toScalaSeq(aggExprs), child, Aggregate$.MODULE$.apply$default$4());
    }
    List<NamedExpression> groupNamed = new ArrayList<>();
    for (Expression e : groupExprs) {
      groupNamed.add((NamedExpression) e);
    }
    return Aggregate$.MODULE$.apply(
        groupSeq, toScalaSeq(groupNamed), child, Aggregate$.MODULE$.apply$default$4());
  }

  private static Repartition buildRepartition(NodeIR node, LogicalPlan child) {
    int numPartitions = 200;
    try {
      String np = node.getAttributes().get("numPartitions");
      if (np != null) numPartitions = Integer.parseInt(np);
    } catch (NumberFormatException ignored) {
    }
    boolean shuffle = "true".equalsIgnoreCase(node.getAttributes().get("shuffle"));
    return new Repartition(numPartitions, shuffle, child);
  }

  private static LogicalPlan buildSubqueryAlias(NodeIR node, LogicalPlan child) {
    String alias = node.getAttributes().getOrDefault("alias", "alias_" + node.getId());
    return SubqueryAlias$.MODULE$.apply(alias, child);
  }

  private static LogicalRelation buildFallbackLeaf(NodeIR node, SparkSession spark) {
    return buildRelation(node, spark);
  }

  // ── Schema and output helpers ─────────────────────────────────────────────

  private static StructType nodeSchema(NodeIR node) {
    List<ColumnIR> cols = node.getOutput();
    if (cols == null || cols.isEmpty()) {
      return new StructType(
          new StructField[] {
            new StructField(
                "_empty", StringType$.MODULE$, true, org.apache.spark.sql.types.Metadata.empty())
          });
    }
    StructField[] fields = new StructField[cols.size()];
    for (int i = 0; i < cols.size(); i++) {
      fields[i] =
          new StructField(
              cols.get(i).getName(),
              irTypeToSparkType(cols.get(i).getDataType()),
              true,
              org.apache.spark.sql.types.Metadata.empty());
    }
    return new StructType(fields);
  }

  private static List<AttributeReference> nodeOutput(NodeIR node) {
    List<ColumnIR> cols = node.getOutput();
    if (cols == null || cols.isEmpty()) return new ArrayList<>();
    List<AttributeReference> result = new ArrayList<>(cols.size());
    for (ColumnIR col : cols) {
      result.add(colToAttr(col));
    }
    return result;
  }

  private static AttributeReference colToAttr(ColumnIR col) {
    return newAttr(col.getName(), irTypeToSparkType(col.getDataType()));
  }

  private static AttributeReference newAttr(String name, DataType dataType) {
    return new AttributeReference(
        name,
        dataType,
        true,
        org.apache.spark.sql.types.Metadata.empty(),
        ExprId.apply(NamedExpression.newExprId().id()),
        toScalaSeq(new ArrayList<String>()));
  }

  private static Seq<AttributeReference> schemaToOutput(StructType schema) {
    List<AttributeReference> list = new ArrayList<>();
    for (StructField f : schema.fields()) {
      list.add(newAttr(f.name(), f.dataType()));
    }
    return toScalaSeq(list);
  }

  private static DataType irTypeToSparkType(String dt) {
    if (dt == null) return StringType$.MODULE$;
    switch (dt) {
      case "long":
        return LongType$.MODULE$;
      case "int":
        return IntegerType$.MODULE$;
      case "double":
        return DoubleType$.MODULE$;
      case "float":
        return FloatType$.MODULE$;
      case "boolean":
        return BooleanType$.MODULE$;
      default:
        return StringType$.MODULE$;
    }
  }

  @SuppressWarnings("unchecked")
  private static List<NamedExpression> mapOutputToChild(
      List<AttributeReference> output, LogicalPlan child) {
    Map<String, AttributeReference> childByName = buildAttrMap(child);

    List<NamedExpression> result = new ArrayList<>();
    int childSize = child.output().size();
    for (int i = 0; i < output.size(); i++) {
      AttributeReference target = output.get(i);
      AttributeReference fromChild = childByName.get(target.name());
      if (fromChild != null) {
        result.add(fromChild);
      } else if (i < childSize) {
        result.add((NamedExpression) child.output().apply(i));
      } else {
        // No matching child column — use the target attribute directly as a placeholder
        result.add(target);
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static List<NamedExpression> mapOutputAliases(
      List<AttributeReference> output, LogicalPlan child) {
    List<NamedExpression> result = new ArrayList<>();
    int childSize = child.output().size();
    for (int i = 0; i < output.size() && i < childSize; i++) {
      AttributeReference target = output.get(i);
      Expression source = (Expression) child.output().apply(i);
      result.add(
          Alias$.MODULE$.apply(
              source,
              target.name(),
              target.exprId(),
              Alias$.MODULE$.apply$default$4(source, target.name()),
              Alias$.MODULE$.apply$default$5(source, target.name()),
              Alias$.MODULE$.apply$default$6(source, target.name())));
    }
    return result;
  }

  private static NamedExpression makeAlias(Expression child, String name) {
    return Alias$.MODULE$.apply(
        child,
        name,
        Alias$.MODULE$.apply$default$3(child, name),
        Alias$.MODULE$.apply$default$4(child, name),
        Alias$.MODULE$.apply$default$5(child, name),
        Alias$.MODULE$.apply$default$6(child, name));
  }

  // ── Condition builders ────────────────────────────────────────────────────

  private static Expression buildCondition(NodeIR node, LogicalPlan child) {
    List<ColumnIR> condCols = node.getConditionColumns();
    if (condCols != null && !condCols.isEmpty()) {
      Map<String, AttributeReference> childByName = buildAttrMap(child);
      Expression combined = null;
      for (ColumnIR col : condCols) {
        AttributeReference attr = childByName.get(col.getName());
        if (attr != null) {
          Expression eq = new EqualTo(attr, attr);
          combined = (combined == null) ? eq : new And(combined, eq);
        }
      }
      if (combined != null) return combined;
    }
    return Literal$.MODULE$.apply((Object) Boolean.TRUE);
  }

  @SuppressWarnings("unchecked")
  private static Option<Expression> buildJoinCondition(
      NodeIR node, LogicalPlan left, LogicalPlan right) {
    List<ColumnIR> condCols = node.getConditionColumns();
    if (condCols == null || condCols.isEmpty()) return Option.empty();

    Map<String, AttributeReference> leftByName = buildAttrMap(left);
    Map<String, AttributeReference> rightByName = buildAttrMap(right);

    Expression combined = null;
    for (ColumnIR col : condCols) {
      AttributeReference l = leftByName.get(col.getName());
      AttributeReference r = rightByName.get(col.getName());
      if (l != null && r != null) {
        Expression eq = new EqualTo(l, r);
        combined = (combined == null) ? eq : new And(combined, eq);
      }
    }

    if (combined != null) return new Some<>(combined);

    if (left.output().size() > 0 && right.output().size() > 0) {
      return new Some<>(
          new EqualTo((Expression) left.output().apply(0), (Expression) right.output().apply(0)));
    }
    return Option.empty();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, AttributeReference> buildAttrMap(LogicalPlan plan) {
    Map<String, AttributeReference> map = new HashMap<>();
    Seq<Attribute> output = plan.output();
    for (int i = 0; i < output.size(); i++) {
      AttributeReference attr = (AttributeReference) output.apply(i);
      map.putIfAbsent(attr.name(), attr);
    }
    return map;
  }

  private static LogicalPlan singleChild(List<LogicalPlan> children, NodeIR node) {
    if (!children.isEmpty()) return children.get(0);
    throw new IllegalStateException(
        "Node " + node.getType() + "#" + node.getId() + " expected children but has none");
  }

  // ── Scala interop helpers ─────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private static <T> Seq<T> toScalaSeq(List<T> list) {
    return (Seq<T>) JavaConverters.asScalaBufferConverter(list).asScala().toList();
  }

  @SuppressWarnings("unchecked")
  private static Seq<NamedExpression> outputAsNamedSeq(LogicalPlan plan) {
    return (Seq<NamedExpression>) (Seq<?>) plan.output();
  }

  @SuppressWarnings("unchecked")
  private static List<NamedExpression> outputToNamedList(LogicalPlan plan) {
    List<NamedExpression> list = new ArrayList<>();
    Seq<Attribute> output = plan.output();
    for (int i = 0; i < output.size(); i++) {
      list.add((NamedExpression) output.apply(i));
    }
    return list;
  }
}
