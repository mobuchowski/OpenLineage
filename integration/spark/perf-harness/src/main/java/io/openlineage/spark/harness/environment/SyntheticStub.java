/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.environment;

import java.util.LinkedHashMap;
import java.util.Map;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.matcher.ElementMatchers;
import org.objenesis.ObjenesisHelper;

/**
 * Builder for dynamically-generated stub objects that extend final or sealed Spark classes.
 *
 * <p>Use this when:
 * <ul>
 *   <li>The target class is {@code final} or sealed and cannot be directly subclassed</li>
 *   <li>The constructor does expensive or impossible work (e.g., requires a live Spark context)
 *       and must be bypassed via Objenesis</li>
 *   <li>You need to override specific methods to return fixed values</li>
 * </ul>
 *
 * <p>For ordinary subclassable types with accessible constructors, prefer {@link Interception#returnStub}.
 *
 * <p>Example:
 * <pre>{@code
 * Object stub = SyntheticStub.extending(SessionCatalog.class)
 *     .method("getTableMetadata", someTableIdentifier)
 *     .method("tableExists", true)
 *     .build();
 * }</pre>
 */
public class SyntheticStub {

  private SyntheticStub() {}

  /** Start building a stub that extends the given base class. */
  public static Builder extending(Class<?> baseClass) {
    return new Builder(baseClass);
  }

  public static class Builder {

    private final Class<?> baseClass;

    /** Method name → fixed return value. */
    private final Map<String, Object> methods = new LinkedHashMap<>();

    private Builder(Class<?> baseClass) {
      this.baseClass = baseClass;
    }

    /**
     * Override a method to return a fixed value.
     *
     * @param methodName the method name to override
     * @param returnValue the value the overriding method should return
     */
    public Builder method(String methodName, Object returnValue) {
      methods.put(methodName, returnValue);
      return this;
    }

    /**
     * Build the stub instance.
     *
     * <p>Uses ByteBuddy to create a dynamic subclass of {@link #baseClass} with
     * {@link FixedValue} implementations for each configured method. The constructor is bypassed
     * via Objenesis so no constructor arguments are needed.
     *
     * @return a new instance of the generated subclass
     */
    public Object build() {
      DynamicType.Builder<?> bb = new ByteBuddy().subclass(baseClass);

      for (Map.Entry<String, Object> entry : methods.entrySet()) {
        bb =
            bb.method(ElementMatchers.named(entry.getKey()))
                .intercept(FixedValue.value(entry.getValue()))
                .modifiers(Visibility.PUBLIC);
      }

      try (DynamicType.Unloaded<?> unloaded = bb.make()) {
        Class<?> stubClass =
            unloaded
                .load(baseClass.getClassLoader())
                .getLoaded();
        return ObjenesisHelper.newInstance(stubClass);
      }
    }
  }
}
