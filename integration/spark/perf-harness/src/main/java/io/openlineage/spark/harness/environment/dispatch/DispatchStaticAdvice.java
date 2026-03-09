/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.environment.dispatch;

import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy Advice for <em>static</em> methods that delegates to {@link InterceptionDispatcher}.
 *
 * <p>Same as {@link DispatchAdvice} but without {@code @Advice.This} (not available for static
 * methods). Passes {@code null} as the {@code self} argument to
 * {@link InterceptionDispatcher#tryIntercept}.
 *
 * <p>Requires {@code InitializationStrategy.NoOp} on the {@code AgentBuilder}.
 */
public class DispatchStaticAdvice {

  @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class, suppress = Throwable.class)
  public static Object enter(
      @Advice.Origin("#t.#m") String key, @Advice.AllArguments Object[] args) {
    return InterceptionDispatcher.tryIntercept(key, null, args);
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(
      @Advice.Enter Object enter, @Advice.Return(readOnly = false) Object returned) {
    if (enter != null) {
      returned = enter;
    }
  }
}
