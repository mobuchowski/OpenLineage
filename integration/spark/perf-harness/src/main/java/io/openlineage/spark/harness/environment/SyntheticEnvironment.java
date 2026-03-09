/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.environment;

import io.openlineage.spark.harness.environment.dispatch.DispatchAdvice;
import io.openlineage.spark.harness.environment.dispatch.DispatchStaticAdvice;
import io.openlineage.spark.harness.environment.dispatch.InterceptionContext;
import io.openlineage.spark.harness.environment.dispatch.InterceptionDispatcher;
import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the set of {@link Interception}s that short-circuit expensive Spark operations during
 * benchmarking.
 *
 * <p>Use {@link #withDefaults()} to get an environment pre-loaded with {@link DefaultRules}, then
 * call {@link #add(Interception)} to layer test-specific overrides on top.
 *
 * <p>{@link #activate()} must be called before {@link org.apache.spark.sql.SparkSession} creation
 * so that classes are instrumented before they are loaded. Implements {@link AutoCloseable} for
 * use in try-with-resources blocks.
 *
 * <p>Example:
 * <pre>{@code
 * try (SyntheticEnvironment env = SyntheticEnvironment.withDefaults()) {
 *     test.configure(env);
 *     env.activate();
 *     // ... create SparkSession, run benchmark ...
 * } // deactivate() called on close
 * }</pre>
 *
 * <p>Note: ByteBuddy self-attach via {@code ByteBuddyAgent.install()} requires
 * {@code --add-opens java.base/java.lang=ALL-UNNAMED} on JDK 9+.
 */
public class SyntheticEnvironment implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(SyntheticEnvironment.class);

  private final List<Interception> interceptions;
  private boolean active = false;

  /** ByteBuddy instrumentation handle — non-null after activate(). */
  private Instrumentation instrumentation;

  /** Transformer installed by activate() — non-null after activate(). */
  private ResettableClassFileTransformer transformer;

  private SyntheticEnvironment(List<Interception> interceptions) {
    this.interceptions = new ArrayList<>(interceptions);
  }

  /** Create an environment pre-loaded with {@link DefaultRules}. */
  public static SyntheticEnvironment withDefaults() {
    return new SyntheticEnvironment(DefaultRules.create());
  }

  /** Create an environment with no interceptions. */
  public static SyntheticEnvironment empty() {
    return new SyntheticEnvironment(new ArrayList<>());
  }

  /**
   * Add an interception. Must be called before {@link #activate()}.
   *
   * @return this (fluent)
   * @throws IllegalStateException if called after {@link #activate()}
   */
  public SyntheticEnvironment add(Interception interception) {
    if (active) {
      throw new IllegalStateException(
          "Cannot add interceptions after SyntheticEnvironment is active. "
              + "Call add() before activate().");
    }
    interceptions.add(interception);
    return this;
  }

  /**
   * Install all interceptions via ByteBuddy self-attach and activate instrumentation.
   *
   * <p>Uses {@code InitializationStrategy.NoOp} — no auxiliary class injection. Dispatcher-based
   * interceptions register handlers in {@link InterceptionDispatcher}; custom-advice interceptions
   * wire their Advice class directly. Idempotent — subsequent calls are no-ops if already active.
   */
  public void activate() {
    if (active) {
      return;
    }

    instrumentation = ByteBuddyAgent.install();
    log.info("[SyntheticEnvironment] ByteBuddy self-attach successful");

    if (interceptions.isEmpty()) {
      log.info("[SyntheticEnvironment] No interceptions configured");
      active = true;
      return;
    }

    AgentBuilder builder =
        new AgentBuilder.Default()
            .with(AgentBuilder.InitializationStrategy.NoOp.INSTANCE);

    for (Interception interception : interceptions) {
      builder = wire(builder, interception);

      if (!interception.hasCustomAdvice()) {
        Function<InterceptionContext, Object> handler = interception.handler();
        for (String method : interception.methodNames()) {
          String key = interception.className() + "." + method;
          InterceptionDispatcher.register(key, handler);
          log.debug("[SyntheticEnvironment] Registered handler: {}", key);
        }
      }

      log.info(
          "[SyntheticEnvironment] Registered interception: {}.{}",
          interception.className(),
          interception.methodNames());
    }

    transformer = builder.installOn(instrumentation);
    log.info("[SyntheticEnvironment] Activated {} interception(s)", interceptions.size());
    active = true;
  }

  private AgentBuilder wire(AgentBuilder builder, Interception interception) {
    String[] methods = interception.methodNames().toArray(new String[0]);
    ElementMatcher.Junction<MethodDescription> methodMatcher =
        ElementMatchers.<MethodDescription>namedOneOf(methods);
    if (interception.isStatic()) {
      methodMatcher = methodMatcher.and(ElementMatchers.isStatic());
    }

    Class<?> adviceClass;
    if (interception.hasCustomAdvice()) {
      adviceClass = interception.adviceClass();
    } else if (interception.isStatic()) {
      adviceClass = DispatchStaticAdvice.class;
    } else {
      adviceClass = DispatchAdvice.class;
    }

    final ElementMatcher.Junction<MethodDescription> finalMatcher = methodMatcher;
    final Class<?> finalAdvice = adviceClass;

    return builder
        .type(ElementMatchers.named(interception.className()))
        .transform(
            (b, typeDescription, classLoader, javaModule, protectionDomain) ->
                b.method(finalMatcher).intercept(Advice.to(finalAdvice)));
  }

  /**
   * Remove all installed ByteBuddy transformers, restore original class behavior, and clear
   * dispatcher handlers. Idempotent.
   */
  public void deactivate() {
    if (!active) {
      return;
    }
    if (transformer != null) {
      transformer.reset(instrumentation, AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
      transformer = null;
    }
    InterceptionDispatcher.clear();
    log.info("[SyntheticEnvironment] Deactivated — all interceptions reset");
    active = false;
  }

  @Override
  public void close() {
    deactivate();
  }

  public boolean isActive() {
    return active;
  }

  public List<Interception> getInterceptions() {
    return interceptions;
  }
}
