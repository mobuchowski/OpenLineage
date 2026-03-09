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
 * ByteBuddy Advice for <em>instance</em> methods that delegates to {@link InterceptionDispatcher}.
 *
 * <p>Inlined into intercepted instance methods via {@code Advice.to(DispatchAdvice.class)}.
 * On enter, calls {@link InterceptionDispatcher#tryIntercept} — a non-null return causes ByteBuddy
 * to skip the original method body ({@code skipOn = OnNonDefaultValue}). On exit, if enter
 * returned a non-null value, it replaces the method's return value.
 *
 * <p>Requires {@code InitializationStrategy.NoOp} on the {@code AgentBuilder}. With NoOp, the
 * only class reference inlined into target bytecode is {@code InterceptionDispatcher}, which is
 * never in a circular class-loading chain with any Spark or Hadoop class.
 *
 * <p>For static methods use {@link DispatchStaticAdvice}.
 */
public class DispatchAdvice {

  @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class, suppress = Throwable.class)
  public static Object enter(
      @Advice.Origin("#t.#m") String key,
      @Advice.This Object self,
      @Advice.AllArguments Object[] args) {
    return InterceptionDispatcher.tryIntercept(key, self, args);
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(
      @Advice.Enter Object enter, @Advice.Return(readOnly = false) Object returned) {
    if (enter != null) {
      returned = enter;
    }
  }
}
