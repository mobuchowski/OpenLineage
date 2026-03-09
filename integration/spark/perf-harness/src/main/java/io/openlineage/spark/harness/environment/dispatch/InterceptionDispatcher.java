/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.environment.dispatch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Central registry that maps interception keys to handler functions.
 *
 * <p>Keys have the form {@code "ClassName.methodName"} as produced by ByteBuddy's
 * {@code @Origin("#t.#m")} expression. Handlers are registered by
 * {@link io.openlineage.spark.harness.environment.SyntheticEnvironment#activate()} and consumed
 * by {@link DispatchAdvice} / {@link DispatchStaticAdvice}.
 *
 * <p>Returning {@code null} from a handler means "don't intercept; run the original method body."
 * Use a sentinel object if you need to return actual {@code null} as the intercepted value.
 */
public class InterceptionDispatcher {

  private static final Map<String, Function<InterceptionContext, Object>> handlers =
      new ConcurrentHashMap<>();

  private InterceptionDispatcher() {}

  /**
   * Try to intercept a method call. Called from {@link DispatchAdvice} and
   * {@link DispatchStaticAdvice} enter methods.
   *
   * @param key {@code "ClassName.methodName"} from {@code @Origin("#t.#m")}
   * @param self receiver object, or {@code null} for static methods
   * @param args method arguments
   * @return handler's return value, or {@code null} if no handler registered or handler returns
   *     {@code null} (meaning "don't intercept")
   */
  public static Object tryIntercept(String key, Object self, Object[] args) {
    Function<InterceptionContext, Object> handler = handlers.get(key);
    if (handler == null) {
      return null;
    }
    return handler.apply(new InterceptionContext(self, args));
  }

  /**
   * Register a handler for the given key.
   *
   * @param key {@code "ClassName.methodName"}
   * @param handler the function to invoke when the method is called; return {@code null} to skip
   */
  public static void register(String key, Function<InterceptionContext, Object> handler) {
    handlers.put(key, handler);
  }

  /** Remove all registered handlers. Called by {@code SyntheticEnvironment.deactivate()}. */
  public static void clear() {
    handlers.clear();
  }
}
