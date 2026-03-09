/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local metrics collected during a single harness run.
 *
 * <p>Two layers of metrics are used by the harness:
 * <ol>
 *   <li><b>Local metrics</b> (this class) — collected in-process by {@link HarnessRunner} using
 *       wall-clock timing and JVM MXBeans.</li>
 *   <li><b>Datadog metrics</b> — collected automatically by dd-java-agent (CPU flame graphs,
 *       allocation profiling, lock contention, APM traces). Available in the DD UI after each run
 *       with no extra code required.</li>
 * </ol>
 */
public class HarnessMetrics {

  private static final Logger log = LoggerFactory.getLogger(HarnessMetrics.class);

  private long planGenerationWallClockMs;
  private long olProcessingWallClockMs;
  private long totalWallClockMs;

  private long gcCollectionsBefore;
  private long gcCollectionsAfter;
  private long gcTimeMsBefore;
  private long gcTimeMsAfter;

  private long memoryUsedBeforeBytes;
  private long memoryUsedAfterBytes;

  private int eventCount;
  private final List<Long> perEventTimingMs = new ArrayList<>();

  // --- Snapshot helpers ---

  /** Capture cumulative GC collections and GC time across all collectors. */
  public static long[] captureGcSnapshot() {
    long collections = 0;
    long timeMs = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      long c = gc.getCollectionCount();
      long t = gc.getCollectionTime();
      if (c >= 0) collections += c;
      if (t >= 0) timeMs += t;
    }
    return new long[] {collections, timeMs};
  }

  /** Capture used heap memory bytes. */
  public static long captureUsedMemoryBytes() {
    Runtime rt = Runtime.getRuntime();
    return rt.totalMemory() - rt.freeMemory();
  }

  // --- Setters (called by HarnessRunner) ---

  public void setPlanGenerationWallClockMs(long ms) {
    this.planGenerationWallClockMs = ms;
  }

  public void setOlProcessingWallClockMs(long ms) {
    this.olProcessingWallClockMs = ms;
  }

  public void setTotalWallClockMs(long ms) {
    this.totalWallClockMs = ms;
  }

  public void setGcBefore(long[] snapshot) {
    this.gcCollectionsBefore = snapshot[0];
    this.gcTimeMsBefore = snapshot[1];
  }

  public void setGcAfter(long[] snapshot) {
    this.gcCollectionsAfter = snapshot[0];
    this.gcTimeMsAfter = snapshot[1];
  }

  public void setMemoryUsedBefore(long bytes) {
    this.memoryUsedBeforeBytes = bytes;
  }

  public void setMemoryUsedAfter(long bytes) {
    this.memoryUsedAfterBytes = bytes;
  }

  public void setEventCount(int count) {
    this.eventCount = count;
  }

  public void addPerEventTiming(long ms) {
    this.perEventTimingMs.add(ms);
  }

  // --- Derived metrics ---

  public long gcCollectionsDelta() {
    return gcCollectionsAfter - gcCollectionsBefore;
  }

  public long gcTimeMsDelta() {
    return gcTimeMsAfter - gcTimeMsBefore;
  }

  public long memoryDeltaBytes() {
    return memoryUsedAfterBytes - memoryUsedBeforeBytes;
  }

  // --- Report ---

  public void printReport(String testName) {
    log.info("=== Harness Metrics: {} ===", testName);
    log.info("  Plan generation   : {} ms", String.format("%,d", planGenerationWallClockMs));
    log.info("  OL processing     : {} ms", String.format("%,d", olProcessingWallClockMs));
    log.info("  Total wall clock  : {} ms", String.format("%,d", totalWallClockMs));
    log.info("  Events emitted    : {}", eventCount);
    log.info("  GC collections    : {} (delta)", gcCollectionsDelta());
    log.info("  GC time           : {} ms (delta)", String.format("%,d", gcTimeMsDelta()));
    log.info("  Memory delta      : {} bytes", String.format("%,d", memoryDeltaBytes()));
    if (!perEventTimingMs.isEmpty()) {
      long sum = perEventTimingMs.stream().mapToLong(Long::longValue).sum();
      log.info("  Per-event timing  : avg={} ms over {} events",
          String.format("%,d", sum / perEventTimingMs.size()), perEventTimingMs.size());
    }
  }

  // --- Getters ---

  public long getPlanGenerationWallClockMs() {
    return planGenerationWallClockMs;
  }

  public long getOlProcessingWallClockMs() {
    return olProcessingWallClockMs;
  }

  public long getTotalWallClockMs() {
    return totalWallClockMs;
  }

  public int getEventCount() {
    return eventCount;
  }

  public List<Long> getPerEventTimingMs() {
    return perEventTimingMs;
  }
}
