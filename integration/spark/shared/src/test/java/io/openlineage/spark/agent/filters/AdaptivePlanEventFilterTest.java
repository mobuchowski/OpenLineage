/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class AdaptivePlanEventFilterTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  AdaptivePlanEventFilter filter = new AdaptivePlanEventFilter(context);
  SparkListenerEvent sparkListenerEvent = mock(SparkListenerEvent.class);
  QueryExecution queryExecution = mock(QueryExecution.class);
  SparkPlan sparkPlan = mock(SparkPlan.class);

  @BeforeEach
  public void setup() {
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.executedPlan()).thenReturn(sparkPlan);
  }

  @Test
  void testNonFinalAdaptivePlanIsFiltered() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      when(sparkPlan.nodeName()).thenReturn("AdaptiveSparkPlan");
      // SparkPlan mock has no isFinalPlan method, so reflection returns false (not final)
      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testFinalAdaptivePlanIsNotFiltered() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      FinalAdaptiveSparkPlan finalPlan = mock(FinalAdaptiveSparkPlan.class);
      when(finalPlan.nodeName()).thenReturn("AdaptiveSparkPlan");
      when(finalPlan.isFinalPlan()).thenReturn(true);
      when(queryExecution.executedPlan()).thenReturn(finalPlan);
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testWhenQueryExecutionIsNull() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      when(context.getQueryExecution()).thenReturn(Optional.ofNullable(null));
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testWhenSparkPlanIsNull() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      when(queryExecution.executedPlan()).thenReturn(null);
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testOtherSparkPlan() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      when(sparkPlan.nodeName()).thenReturn("OtherSparkPlan");
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testNonDeltaPlan() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(false);
      when(sparkPlan.nodeName()).thenReturn("AdaptiveSparkPlan");
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  /** Test helper that extends SparkPlan with isFinalPlan method for reflection-based access. */
  abstract static class FinalAdaptiveSparkPlan extends SparkPlan {
    public abstract boolean isFinalPlan();
  }
}
