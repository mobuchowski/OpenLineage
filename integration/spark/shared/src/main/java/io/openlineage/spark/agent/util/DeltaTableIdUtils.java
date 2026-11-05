/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.OneLakeLocation;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;

/**
 * Reads a Delta table's ID ({@code metaData.id} in its Delta log) from a snapshot Delta has already
 * loaded, without reading the table again.
 *
 * <p>Delta's classes are used by reflection, because they aren't always on the classpath and their
 * methods differ between Delta versions.
 */
@Slf4j
public final class DeltaTableIdUtils {
  private DeltaTableIdUtils() {}

  /**
   * The table ID behind a Delta file index ({@code TahoeLogFileIndex} or another {@code
   * TahoeFileIndex}).
   *
   * @return the ID, or empty if this isn't a Delta file index or no snapshot is loaded
   */
  public static Optional<String> fromFileIndex(Object fileIndex) {
    if (fileIndex == null || !fileIndex.getClass().getName().contains(".delta.")) {
      return Optional.empty();
    }
    return invoke(fileIndex, "deltaLog").flatMap(DeltaTableIdUtils::fromDeltaLog);
  }

  /**
   * The table ID of a Delta table in a catalog ({@code DeltaTableV2}).
   *
   * @return the ID, or empty if this isn't a Delta table or no snapshot is loaded
   */
  public static Optional<String> fromTable(Object table) {
    if (table == null || !table.getClass().getName().endsWith(".DeltaTableV2")) {
      return Optional.empty();
    }
    return invoke(table, "deltaLog").flatMap(DeltaTableIdUtils::fromDeltaLog);
  }

  static Optional<String> fromDeltaLog(Object deltaLog) {
    // unsafeVolatileSnapshot (Delta 2.2 and later) returns the latest loaded snapshot without
    // reading the log; older versions call it snapshot
    Optional<Object> snapshot = invoke(deltaLog, "unsafeVolatileSnapshot");
    if (!snapshot.isPresent()) {
      snapshot = invoke(deltaLog, "snapshot");
    }
    // a table that doesn't exist yet has version -1 and a made-up ID
    return snapshot
        .filter(s -> invoke(s, "version").map(v -> ((Number) v).longValue() >= 0).orElse(false))
        .flatMap(s -> invoke(s, "metadata"))
        .flatMap(metadata -> invoke(metadata, "id"))
        .map(Object::toString)
        .map(DeltaTableIdUtils::normalize);
  }

  // Fabric warehouses write their table IDs in upper case and Spark in lower case; the same ID
  // must always be written the same way
  private static String normalize(String id) {
    return OneLakeLocation.guid(id).map(UUID::toString).orElse(id);
  }

  private static Optional<Object> invoke(Object target, String method) {
    if (MethodUtils.getAccessibleMethod(target.getClass(), method) == null) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(MethodUtils.invokeMethod(target, method));
    } catch (ReflectiveOperationException | RuntimeException e) {
      log.debug("Cannot call {} on {}", method, target.getClass().getName(), e);
      return Optional.empty();
    }
  }
}
