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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.HarnessBridge$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates a single performance harness run.
 *
 * <h2>No compile-time OL dependency</h2>
 *
 * <p>The harness must NOT import OL classes at compile time. This allows backtesting against
 * previous OL versions without recompiling the harness:
 *
 * <pre>{@code
 * # Test current code
 * ./gradlew :perf-harness:runHarness -PolJar=../app/build/libs/openlineage-spark-agent_2.13-1.45.0-SNAPSHOT-shadow.jar
 *
 * # Compare against released 1.30
 * ./gradlew :perf-harness:runHarness -PolJar=/path/to/openlineage-spark_2.13-1.30.0.jar -PdataVersion=ol-1.30
 * }</pre>
 *
 * <p>The OL listener is registered by class name via {@code spark.extraListeners}. Spark
 * instantiates it from the runtime classpath (which includes the olRuntime configuration).
 * The harness interacts with OL only through Spark's listener mechanism and HTTP event capture.
 *
 * <h2>Run lifecycle</h2>
 * <ol>
 *   <li>Start {@link EventCaptureServer} on a random port</li>
 *   <li>Create and activate {@link SyntheticEnvironment} (ByteBuddy self-attach)</li>
 *   <li>Create SparkSession with OL listener registered by class name</li>
 *   <li>Call {@link HarnessTest#generatePlan(SparkSession)} → LogicalPlan</li>
 *   <li>Create QueryExecution via HarnessBridge, inject into Spark's execution map</li>
 *   <li>Post {@code SparkListenerSQLExecutionStart} → triggers OL processing</li>
 *   <li>Wait for listener bus to drain</li>
 *   <li>Post {@code SparkListenerSQLExecutionEnd} → triggers OL COMPLETE event</li>
 *   <li>Wait for listener bus to drain</li>
 *   <li>Collect {@link HarnessMetrics}, remove injected QueryExecution</li>
 *   <li>Report results to stdout (+ Datadog APM when agent is attached)</li>
 * </ol>
 */
public class HarnessRunner {

  private static final Logger log = LoggerFactory.getLogger(HarnessRunner.class);

  /** Spark configuration key for the OL listener. Registered by class name — no import needed. */
  private static final String OL_LISTENER_CLASS =
      "io.openlineage.spark.agent.OpenLineageSparkListener";

  /** Listener bus drain timeout in milliseconds. */
  private static final long LISTENER_DRAIN_TIMEOUT_MS = 60_000L;

  private final List<HarnessTest> tests;

  public HarnessRunner(List<HarnessTest> tests) {
    this.tests = tests;
  }

  public HarnessRunner(HarnessTest... tests) {
    this(Arrays.asList(tests));
  }

  /**
   * Run all registered tests using a pre-activated {@link SyntheticEnvironment}.
   *
   * <p>Called from {@link HarnessLauncher} after ByteBuddy instrumentation is installed.
   * The environment must already be active — tests are run without re-activating it.
   */
  public void runAll(SyntheticEnvironment env) {
    for (HarnessTest test : tests) {
      run(test, env);
    }
  }

  /**
   * Run a single test using a pre-activated {@link SyntheticEnvironment}.
   *
   * <p>The caller (typically {@link HarnessLauncher}) is responsible for activating the
   * environment before any Spark class is loaded. This method assumes instrumentation is
   * already installed and does not call {@link SyntheticEnvironment#activate()}.
   */
  public HarnessMetrics run(HarnessTest test, SyntheticEnvironment env) {
    HarnessMetrics metrics = new HarnessMetrics();
    String testName = test.getClass().getSimpleName();

    EventCaptureServer captureServer;
    try {
      captureServer = new EventCaptureServer();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start event capture server", e);
    }

    log.info("[HarnessRunner] Starting test: {}", testName);

    try {
      SparkSession spark;
      try {
        spark = buildSparkSession(captureServer.getPort());
      } catch (Exception e) {
        log.error("[HarnessRunner] Failed to build SparkSession for test: {}", testName, e);
        throw new RuntimeException("Failed to build SparkSession for test: " + testName, e);
      }

      try {
        runBenchmark(test, spark, captureServer, metrics);
      } finally {
        // Stop Spark first — this triggers SparkListenerApplicationEnd, which causes OL to emit
        // a final COMPLETE event. Count events only after Spark is fully stopped so we capture all
        // OL emissions (including the shutdown-triggered one).
        spark.stop();
        metrics.setEventCount(captureServer.getEvents().size());
      }
    } finally {
      captureServer.stop();
    }

    metrics.printReport(testName);
    return metrics;
  }

  /**
   * Core benchmark loop — assumes SparkSession and SyntheticEnvironment are already live.
   */
  private void runBenchmark(
      HarnessTest test,
      SparkSession spark,
      EventCaptureServer captureServer,
      HarnessMetrics metrics) {

    long runStart = System.currentTimeMillis();

    // ── Step 1: Generate logical plan ──────────────────────────────────────
    long[] gcBefore = HarnessMetrics.captureGcSnapshot();
    long memBefore = HarnessMetrics.captureUsedMemoryBytes();

    long planStart = System.currentTimeMillis();
    LogicalPlan plan = test.generatePlan(spark);
    metrics.setPlanGenerationWallClockMs(System.currentTimeMillis() - planStart);

    log.info("[HarnessRunner] Plan generated: {} ms", metrics.getPlanGenerationWallClockMs());

    // ── Step 2: Create QueryExecution + inject into Spark's execution map ──
    QueryExecution qe = HarnessBridge$.MODULE$.createQueryExecution(spark, plan);
    long executionId = HarnessBridge$.MODULE$.nextExecutionId();
    HarnessBridge$.MODULE$.injectQueryExecution(executionId, qe);

    try {
      // ── Step 3: Post events → trigger OL processing ──────────────────────
      SparkListenerSQLExecutionStart startEvent =
          HarnessBridge$.MODULE$.createExecutionStartEvent(executionId, qe);
      SparkListenerSQLExecutionEnd endEvent =
          HarnessBridge$.MODULE$.createExecutionEndEvent(executionId);

      long olStart = System.currentTimeMillis();

      HarnessBridge$.MODULE$.postEvent(spark.sparkContext(), startEvent);
      HarnessBridge$.MODULE$.drainListenerBus(spark.sparkContext(), LISTENER_DRAIN_TIMEOUT_MS);

      HarnessBridge$.MODULE$.postEvent(spark.sparkContext(), endEvent);
      HarnessBridge$.MODULE$.drainListenerBus(spark.sparkContext(), LISTENER_DRAIN_TIMEOUT_MS);

      metrics.setOlProcessingWallClockMs(System.currentTimeMillis() - olStart);

      log.info("[HarnessRunner] OL processing: {} ms, events captured: {}",
          metrics.getOlProcessingWallClockMs(), captureServer.getEvents().size());

    } finally {
      HarnessBridge$.MODULE$.removeQueryExecution(executionId);
    }

    // ── Step 4: Collect final metrics ──────────────────────────────────────
    metrics.setGcBefore(gcBefore);
    metrics.setGcAfter(HarnessMetrics.captureGcSnapshot());
    metrics.setMemoryUsedBefore(memBefore);
    metrics.setMemoryUsedAfter(HarnessMetrics.captureUsedMemoryBytes());
    // Note: setEventCount() is called in run() after spark.stop() to capture shutdown events.
    metrics.setTotalWallClockMs(System.currentTimeMillis() - runStart);
  }

  /**
   * Build a SparkSession with the OL listener registered by class name and HTTP transport
   * pointed at the local event capture server.
   *
   * <p>The OL JAR is on the runtime classpath ({@code olRuntime} configuration in build.gradle).
   * Spark instantiates the listener from that classpath without any compile-time OL import.
   */
  private SparkSession buildSparkSession(int capturePort) {
    String transportUrl = "http://localhost:" + capturePort;
    return SparkSession.builder()
        .master("local[*]")
        .appName("ol-perf-harness")
        .config("spark.extraListeners", OL_LISTENER_CLASS)
        // OL HTTP transport: POST events to the local capture server
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", transportUrl)
        // Namespace identifies the harness environment in OL events
        .config("spark.openlineage.namespace", "harness")
        // Disable Spark UI to reduce startup overhead
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        // Reduce Spark's log noise during harness runs
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate();
  }
}
