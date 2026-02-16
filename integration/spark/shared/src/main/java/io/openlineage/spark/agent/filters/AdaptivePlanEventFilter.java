/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.isDeltaPlan;

import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;

/**
 * Filters out intermediate Adaptive Query Execution (AQE) plans when Delta is configured.
 *
 * <p>Only intermediate (non-final) AQE plans are filtered, as they represent re-optimization steps
 * that can produce duplicate events. Final AQE plans represent the actual execution and should
 * always emit lineage events.
 */
@Slf4j
public class AdaptivePlanEventFilter implements EventFilter {

  private final OpenLineageContext context;

  public AdaptivePlanEventFilter(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    if (!isDeltaPlan()) {
      return false;
    }

    return context
        .getQueryExecution()
        .map(QueryExecution::executedPlan)
        .filter(sparkPlan -> sparkPlan.nodeName().contains("AdaptiveSparkPlan"))
        .filter(sparkPlan -> !isFinalPlan(sparkPlan))
        .isPresent();
  }

  /**
   * Uses reflection to check the isFinalPlan property on AdaptiveSparkPlanExec. If the plan is
   * final, it represents the actual execution and should not be filtered.
   */
  private boolean isFinalPlan(SparkPlan sparkPlan) {
    try {
      return (boolean) sparkPlan.getClass().getMethod("isFinalPlan").invoke(sparkPlan);
    } catch (Exception e) {
      log.debug("Could not determine isFinalPlan for {}, assuming not final", sparkPlan, e);
      return false;
    }
  }
}
