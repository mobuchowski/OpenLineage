/** 
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{QueryExecution, SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

/**
 * Bridge class in the org.apache.spark.sql package to access private[sql] and private[spark]
 * members of SQLExecution and SparkContext.
 *
 * SQLExecution.executionIdToQueryExecution is private[sql] in Spark 4 — accessible to code
 * within the org.apache.spark.sql package scope (including sub-packages). This object lives in
 * that package so the harness can inject synthetic QueryExecution instances without reflection.
 *
 * SparkContext.listenerBus is private[spark] — accessible from org.apache.spark.* subpackages.
 * The postEvent() and drainListenerBus() helpers expose it to HarnessRunner.
 *
 * Spark 4 notes:
 *  - QueryExecution moved from org.apache.spark.sql to org.apache.spark.sql.execution
 *  - nextExecutionId is private (not private[sql]) in Spark 4 — we use our own counter instead,
 *    starting at Long.MaxValue/2 to avoid collisions with real Spark execution IDs
 *  - executionIdToQueryExecution remains private[sql] — accessible from this package
 *
 * Usage from Java:
 *   long id = HarnessBridge$.MODULE$.nextExecutionId();
 *   QueryExecution qe = HarnessBridge$.MODULE$.createQueryExecution(spark, plan);
 *   HarnessBridge$.MODULE$.injectQueryExecution(id, qe);
 *   HarnessBridge$.MODULE$.postEvent(spark.sparkContext(), startEvent);
 *   HarnessBridge$.MODULE$.drainListenerBus(spark.sparkContext(), 30_000L);
 *   HarnessBridge$.MODULE$.removeQueryExecution(id);
 */
object HarnessBridge {

  // Use our own counter rather than SQLExecution.nextExecutionId (private in Spark 4).
  // Start at Long.MaxValue / 2 to avoid collisions with real Spark execution IDs.
  private val _testExecutionId = new AtomicLong(Long.MaxValue / 2)

  /** Obtain the next synthetic execution ID for use with injectQueryExecution. */
  def nextExecutionId(): Long = _testExecutionId.getAndIncrement()

  /** Register a synthetic QueryExecution so OL's ContextFactory can find it via
   *  SQLExecution.getQueryExecution(executionId). */
  def injectQueryExecution(executionId: Long, qe: QueryExecution): Unit =
    SQLExecution.executionIdToQueryExecution.put(executionId, qe)

  /** Remove the synthetic QueryExecution after the harness run is complete. */
  def removeQueryExecution(executionId: Long): Unit =
    SQLExecution.executionIdToQueryExecution.remove(executionId)

  // ── Phase 4 helpers ──────────────────────────────────────────────────────

  /**
   * Create a QueryExecution for the given plan within the given SparkSession.
   * Called from HarnessRunner before injecting into the execution map.
   */
  def createQueryExecution(session: SparkSession, plan: LogicalPlan): QueryExecution = {
    // Mark the plan (and all children) as already analyzed so that Analyzer.executeAndCheck
    // returns immediately on the `if (plan.analyzed) return plan` fast-path.
    // Without this, OL's call to queryExecution.analyzed triggers the full Catalyst
    // analysis pipeline even though our synthetic plans are structurally resolved.
    // setAnalyzed() is private[sql] — accessible here because HarnessBridge lives in
    // org.apache.spark.sql.
    plan.setAnalyzed()
    // In Spark 4, org.apache.spark.sql.SparkSession is an API trait;
    // QueryExecution takes the concrete org.apache.spark.sql.classic.SparkSession.
    // SparkSession.builder().getOrCreate() always returns the classic implementation.
    val classicSession = session.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    new QueryExecution(classicSession, plan)
  }

  /**
   * Build a minimal SparkListenerSQLExecutionStart event for the given execution ID.
   * OL's OpenLineageSparkListener uses executionId to look up the QueryExecution via
   * SQLExecution.getQueryExecution — the other fields are metadata only.
   */
  def createExecutionStartEvent(
      executionId: Long,
      qe: QueryExecution): SparkListenerSQLExecutionStart = {
    val stubPlanInfo = new SparkPlanInfo(
      "harness-synthetic-plan",
      "harness-synthetic-plan",
      Seq.empty,
      Map.empty,
      Seq.empty)
    SparkListenerSQLExecutionStart(
      executionId = executionId,
      rootExecutionId = Some(executionId),
      description = s"harness-run-$executionId",
      details = "OpenLineage Spark Performance Harness synthetic execution",
      physicalPlanDescription = "synthetic",
      sparkPlanInfo = stubPlanInfo,
      time = System.currentTimeMillis())
  }

  /**
   * Build a minimal SparkListenerSQLExecutionEnd event.
   * OL's listener uses this to trigger the "end" lifecycle (emit RunEvent COMPLETE).
   */
  def createExecutionEndEvent(executionId: Long): SparkListenerSQLExecutionEnd =
    SparkListenerSQLExecutionEnd(
      executionId = executionId,
      time = System.currentTimeMillis(),
      errorMessage = Some(""))

  /**
   * Post a SparkListenerEvent to the listener bus.
   * SparkContext.listenerBus is private[spark] — accessible from this package.
   */
  def postEvent(sc: SparkContext, event: SparkListenerEvent): Unit =
    sc.listenerBus.post(event)

  /**
   * Block until the listener bus queue is empty or the timeout elapses.
   * SparkContext.listenerBus is private[spark] — accessible from this package.
   *
   * @param timeoutMs timeout in milliseconds
   */
  def drainListenerBus(sc: SparkContext, timeoutMs: Long): Unit =
    sc.listenerBus.waitUntilEmpty(timeoutMs)
}
