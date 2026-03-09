/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.harness.plan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.openlineage.spark.harness.plan.ir.PlanIR;
import io.openlineage.spark.harness.plan.parser.SparkJsonParser;
import io.openlineage.spark.harness.plan.parser.TreeStringParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * CLI tool to convert serialized Spark logical plans into the harness IR format.
 *
 * <p>Usage:
 * <pre>
 * # Convert treeString format (default):
 * java -cp perf-harness.jar io.openlineage.spark.harness.plan.PlanConverter \
 *     input-plan.txt output-plan.json
 *
 * # Convert Spark JSON format:
 * java -cp perf-harness.jar io.openlineage.spark.harness.plan.PlanConverter \
 *     --json input-plan.json output-plan.json
 *
 * # Auto-detect format:
 * java -cp perf-harness.jar io.openlineage.spark.harness.plan.PlanConverter \
 *     --auto input-file output-plan.json
 * </pre>
 *
 * <p>Or via Gradle:
 * <pre>
 * ./gradlew :perf-harness:convertPlan -PplanInput=input.txt -PplanOutput=output.json
 * </pre>
 */
public class PlanConverter {

  private static final ObjectMapper mapper =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: PlanConverter [--json|--text|--auto] <input-file> <output-file>");
      System.err.println();
      System.err.println("  --text   Parse as Spark treeString text (default)");
      System.err.println("  --json   Parse as Spark plan.toJSON() output");
      System.err.println("  --auto   Auto-detect format from file content");
      System.exit(1);
    }

    String format = "text";
    int fileArgStart = 0;

    if (args[0].startsWith("--")) {
      format = args[0].substring(2);
      fileArgStart = 1;
    }

    Path inputPath = Paths.get(args[fileArgStart]);
    Path outputPath = Paths.get(args[fileArgStart + 1]);

    if (!Files.exists(inputPath)) {
      System.err.println("Input file not found: " + inputPath);
      System.exit(1);
    }

    if (format.equals("auto")) {
      format = detectFormat(inputPath);
      System.out.println("Detected format: " + format);
    }

    PlanIR ir;
    switch (format) {
      case "text":
        ir = new TreeStringParser().parseFile(inputPath);
        break;
      case "json":
        ir = new SparkJsonParser().parseFile(inputPath);
        break;
      default:
        System.err.println("Unknown format: " + format);
        System.exit(1);
        return;
    }

    mapper.writeValue(outputPath.toFile(), ir);

    System.out.println("Converted plan:");
    System.out.println("  Nodes:       " + ir.nodeCount());
    System.out.println("  Max depth:   " + ir.maxDepth());
    System.out.println(
        "  Leaf nodes:  "
            + ir.getNodes().stream().filter(n -> n.getChildren().isEmpty()).count());
    System.out.println(
        "  Total cols:  "
            + ir.getNodes().stream().mapToInt(n -> n.getOutput().size()).sum());
    System.out.println(
        "  Truncated:   "
            + ir.getNodes().stream().mapToInt(n -> n.getTruncatedColumns()).sum()
            + " columns");
    System.out.println("  Output:      " + outputPath);
  }

  /**
   * Auto-detect whether the input file is Spark JSON or treeString text.
   */
  static String detectFormat(Path path) throws IOException {
    // Read the first non-empty line to check format
    try (Stream<String> lines = Files.lines(path)) {
      String firstLine =
          lines.filter(l -> !l.trim().isEmpty()).findFirst().orElse("");
      // Spark JSON starts with [ (JSON array)
      if (firstLine.trim().startsWith("[")) {
        return "json";
      }
      return "text";
    }
  }
}
