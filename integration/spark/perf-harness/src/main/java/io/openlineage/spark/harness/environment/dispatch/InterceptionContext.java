/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.environment.dispatch;

/**
 * Holds the context for a single method interception: the receiver object and its arguments.
 *
 * <p>Passed to handler lambdas registered with {@link InterceptionDispatcher}.
 *
 * @see #self() receiver object (unchecked cast helper)
 * @see #arg(int) argument by index (unchecked cast helper)
 */
public final class InterceptionContext {

  private final Object rawSelf;
  private final Object[] args;

  public InterceptionContext(Object self, Object[] args) {
    this.rawSelf = self;
    this.args = args;
  }

  /**
   * Returns the receiver object cast to the requested type. {@code null} for static methods.
   *
   * @throws ClassCastException if the actual type does not match
   */
  @SuppressWarnings("unchecked")
  public <T> T self() {
    return (T) rawSelf;
  }

  /**
   * Returns the argument at position {@code index} cast to the requested type.
   *
   * @throws ClassCastException if the actual type does not match
   * @throws ArrayIndexOutOfBoundsException if index is out of range
   */
  @SuppressWarnings("unchecked")
  public <T> T arg(int index) {
    return (T) args[index];
  }
}
