/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness;

import io.openlineage.spark.harness.environment.SyntheticEnvironment;
import io.openlineage.spark.harness.tests.GcpBillingPipelineTest;
import io.openlineage.spark.harness.tests.PathInterceptionSmokeTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified entry point for the OpenLineage Spark performance harness.
 *
 * <p>Select the test scenario via the {@code harness.test} system property:
 * <ul>
 *   <li>{@code smoke} — {@link PathInterceptionSmokeTest}: 1 leaf, ~15s (default)</li>
 *   <li>{@code gcp} — {@link GcpBillingPipelineTest}: 310 leaves, ~70s</li>
 * </ul>
 *
 * <h2>Why a separate launcher?</h2>
 *
 * <p>ByteBuddy instrumentation must be activated <em>before</em> any Spark class is loaded.
 * {@link HarnessRunner} imports Spark classes ({@code QueryExecution}, {@code SparkSession}, etc.)
 * at its class-definition level, so the JVM may load those classes when {@code HarnessRunner}
 * itself is loaded — before any instance method can call {@code env.activate()}.
 *
 * <p>This launcher has <strong>no Spark imports</strong>. It activates ByteBuddy first, then
 * delegates to {@link HarnessRunner} only after instrumentation is installed. By the time Spark
 * classes are resolved (when {@code HarnessRunner} executes), the {@code ClassFileTransformer} is
 * already registered and intercepts their loading or retransforms them safely.
 *
 * <h2>Activation order</h2>
 * <ol>
 *   <li>Create {@link SyntheticEnvironment} and configure test-specific rules</li>
 *   <li>Call {@link SyntheticEnvironment#activate()} — installs ByteBuddy transformer</li>
 *   <li>Load and run {@link HarnessRunner} — Spark classes load after this point</li>
 * </ol>
 */
public class HarnessLauncher {

  private static final Logger log = LoggerFactory.getLogger(HarnessLauncher.class);

  /** Entry point for {@code ./gradlew runHarness} and {@code ./gradlew runSmoke}. */
  public static void main(String[] args) throws InterruptedException {
    String olJar = System.getProperty("ol.jar.path", "(not set)");
    String testName = System.getProperty("harness.test", "gcp");
    log.info("OpenLineage Spark Performance Harness");
    log.info("Spark  : {}", System.getProperty("spark.version", "?"));
    log.info("OL JAR : {}", olJar);
    log.info("Test   : {}", testName);

    HarnessTest test = createTest(testName);

    // Activate ByteBuddy BEFORE any Spark/Hadoop class is loaded.
    // HarnessRunner (which imports QueryExecution etc.) is not referenced until after activate().
    try (SyntheticEnvironment env = SyntheticEnvironment.withDefaults()) {
      env.activate();
      // Give the Datadog profiler (start.delay=1) time to warm up before the benchmark starts.
      // 5s pre-run sleep ensures the profiler is actively recording when OL processing begins.
      Thread.sleep(5000);
      test.configure(env);
      new HarnessRunner(test).runAll(env);
    }
    // Allow the Datadog profiler time to complete its upload before the JVM exits.
    // Default 5s is sufficient for local runs (JFR upload.period=5s).
    // Docker runs set harness.profiler.wait.ms=20000 to ensure start.delay + upload completes.
    long profilerWaitMs = Long.parseLong(System.getProperty("harness.profiler.wait.ms", "5000"));
    Thread.sleep(profilerWaitMs);
  }

  private static HarnessTest createTest(String name) {
    switch (name) {
      case "smoke":
        return new PathInterceptionSmokeTest();
      case "gcp":
        return new GcpBillingPipelineTest();
      default:
        throw new IllegalArgumentException(
            "Unknown harness.test value: '" + name + "'. Valid values: smoke, gcp");
    }
  }
}
