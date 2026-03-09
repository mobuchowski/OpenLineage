/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.environment;

import io.openlineage.spark.harness.environment.dispatch.InterceptionContext;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Declarative descriptor for a single method interception.
 *
 * <p>Use the factory methods {@link #on} and {@link #onStatic} to create an interception, then
 * chain one of the handler modes ({@link #returnConstant}, {@link #returnStub}, {@link #handle},
 * {@link #advice}) to define behavior. Optionally add a {@link #guard} predicate to filter
 * invocations before the handler runs.
 *
 * <p>Example — return a constant:
 * <pre>{@code
 * Interception.on("org.apache.spark.sql.execution.datasources.InMemoryFileIndex", "inputFiles")
 *     .returnConstant(new String[0]);
 * }</pre>
 *
 * <p>Example — guard + stub:
 * <pre>{@code
 * Interception.onStatic("org.apache.hadoop.fs.FileSystem", "get")
 *     .guard(ctx -> "synthetic-cluster".equals(ctx.<URI>arg(0).getHost()))
 *     .returnStub("io.openlineage.spark.harness.synthetic.SyntheticHadoopFileSystem");
 * }</pre>
 *
 * <p>Example — arbitrary handler:
 * <pre>{@code
 * Interception.on("org.apache.spark.sql.execution.QueryExecution", "optimizedPlan", "analyzed")
 *     .handle(ctx -> {
 *         QueryExecution qe = ctx.self();
 *         LogicalPlan plan = qe.logical();
 *         return (plan != null && plan.resolved()) ? plan : null;
 *     });
 * }</pre>
 */
public class Interception {

  private final String className;
  private final List<String> methodNames;
  private final boolean isStatic;

  /** Handler function — {@code null} when using custom advice. */
  private Function<InterceptionContext, Object> handler;

  /** Custom ByteBuddy Advice class — {@code null} when using the dispatcher. */
  private Class<?> customAdviceClass;

  /** Optional guard predicate applied before the handler. */
  private Predicate<InterceptionContext> predicate;

  private Interception(String className, List<String> methodNames, boolean isStatic) {
    this.className = className;
    this.methodNames = methodNames;
    this.isStatic = isStatic;
  }

  /** Create an instance-method interception for the given class and method names. */
  public static Interception on(String className, String... methodNames) {
    return new Interception(className, Arrays.asList(methodNames), false);
  }

  /** Create a static-method interception for the given class and method names. */
  public static Interception onStatic(String className, String... methodNames) {
    return new Interception(className, Arrays.asList(methodNames), true);
  }

  /**
   * Return a fixed constant value from the intercepted method.
   *
   * <p>Equivalent to {@code handle(ctx -> value)}.
   */
  public Interception returnConstant(Object value) {
    this.handler = ctx -> value;
    return this;
  }

  /**
   * Return a lazily-created stub instance from the intercepted method.
   *
   * <p>The stub class is loaded and instantiated on first call via reflection. The result is
   * cached in an {@link AtomicReference} — subsequent calls return the cached instance without
   * synchronization. Thread-safe via {@link AtomicReference#compareAndSet}.
   *
   * @param stubClassName fully-qualified class name of the stub
   * @param ctorArgs constructor arguments (empty for no-arg constructor)
   */
  public Interception returnStub(String stubClassName, Object... ctorArgs) {
    this.handler = new LazyStubHandler(stubClassName, ctorArgs);
    return this;
  }

  /**
   * Apply an arbitrary handler function. Return {@code null} from the handler to skip
   * interception (run the original method body).
   */
  public Interception handle(Function<InterceptionContext, Object> h) {
    this.handler = h;
    return this;
  }

  /**
   * Use a custom ByteBuddy Advice class instead of the dispatcher.
   *
   * <p>Use this escape hatch when the dispatcher pattern cannot express the logic — for example,
   * enter/exit state coordination or per-invocation caching with {@code @Advice.Return} in exit.
   * With {@code InitializationStrategy.NoOp}, custom Advice classes can reference domain types
   * directly without reflection.
   */
  public Interception advice(Class<?> adviceClass) {
    this.customAdviceClass = adviceClass;
    return this;
  }

  /**
   * Add a guard predicate. The handler only runs if the predicate returns {@code true};
   * otherwise the interception is skipped ({@code null} returned to ByteBuddy).
   *
   * <p>{@code guard()} may be called before or after setting the handler. The predicate is
   * combined with the handler lazily in {@link #handler()}.
   */
  public Interception guard(Predicate<InterceptionContext> predicate) {
    this.predicate = predicate;
    return this;
  }

  public String className() {
    return className;
  }

  public List<String> methodNames() {
    return methodNames;
  }

  public boolean isStatic() {
    return isStatic;
  }

  public boolean hasCustomAdvice() {
    return customAdviceClass != null;
  }

  public Class<?> adviceClass() {
    return customAdviceClass;
  }

  /**
   * Returns the effective handler function, with the guard predicate applied if one was set.
   * Returns {@code null} if no handler was set (custom advice path).
   */
  public Function<InterceptionContext, Object> handler() {
    if (predicate == null || handler == null) {
      return handler;
    }
    final Function<InterceptionContext, Object> h = handler;
    final Predicate<InterceptionContext> p = predicate;
    return ctx -> p.test(ctx) ? h.apply(ctx) : null;
  }

  /**
   * Thread-safe lazy factory for stub instances.
   *
   * <p>Loads the stub class and calls its constructor on first invocation. The result is cached
   * in an {@link AtomicReference} — subsequent calls return the cached instance without
   * synchronization. This replaces the double-checked locking pattern used in the old rule
   * classes.
   */
  static class LazyStubHandler implements Function<InterceptionContext, Object> {

    private final String stubClassName;
    private final Object[] ctorArgs;
    private final AtomicReference<Object> cached = new AtomicReference<>();

    LazyStubHandler(String stubClassName, Object[] ctorArgs) {
      this.stubClassName = stubClassName;
      this.ctorArgs = ctorArgs;
    }

    @Override
    public Object apply(InterceptionContext ctx) {
      Object instance = cached.get();
      if (instance != null) {
        return instance;
      }
      Object created = create();
      cached.compareAndSet(null, created);
      return cached.get();
    }

    private Object create() {
      try {
        Class<?> cls = Class.forName(stubClassName);
        if (ctorArgs.length == 0) {
          return cls.getDeclaredConstructor().newInstance();
        }
        Class<?>[] paramTypes = new Class<?>[ctorArgs.length];
        for (int i = 0; i < ctorArgs.length; i++) {
          paramTypes[i] = ctorArgs[i].getClass();
        }
        return cls.getDeclaredConstructor(paramTypes).newInstance(ctorArgs);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to create stub instance of " + stubClassName + " — is it on the classpath?",
            e);
      }
    }
  }
}
