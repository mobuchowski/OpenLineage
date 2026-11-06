/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DeltaTableIdUtilsTest {
  @Test
  void testTableIdOfLoadedSnapshot() {
    // Fabric warehouses write upper case IDs
    assertThat(
            DeltaTableIdUtils.fromDeltaLog(
                new FakeDeltaLog(new FakeSnapshot(3, "0D64173F-C091-4E28-B6A5-322260A5F12C"))))
        .contains("0d64173f-c091-4e28-b6a5-322260a5f12c");
    assertThat(DeltaTableIdUtils.fromDeltaLog(new FakeDeltaLog(new FakeSnapshot(0, "not-a-guid"))))
        .contains("not-a-guid");
  }

  @Test
  void testNoTableIdBeforeTheTableExists() {
    assertThat(
            DeltaTableIdUtils.fromDeltaLog(
                new FakeDeltaLog(new FakeSnapshot(-1, "0d64173f-c091-4e28-b6a5-322260a5f12c"))))
        .isEmpty();
  }

  @Test
  void testOnlyForDeltaClasses() {
    assertThat(DeltaTableIdUtils.fromFileIndex(new Object())).isEmpty();
    assertThat(DeltaTableIdUtils.fromFileIndex(null)).isEmpty();
    assertThat(DeltaTableIdUtils.fromTable(new Object())).isEmpty();
  }

  public static class FakeDeltaLog {
    private final FakeSnapshot snapshot;

    FakeDeltaLog(FakeSnapshot snapshot) {
      this.snapshot = snapshot;
    }

    public FakeSnapshot unsafeVolatileSnapshot() {
      return snapshot;
    }
  }

  // named like Delta's own methods, which are called by reflection
  @SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
  public static class FakeSnapshot {
    private final long version;
    private final String id;

    FakeSnapshot(long version, String id) {
      this.version = version;
      this.id = id;
    }

    public long version() {
      return version;
    }

    public FakeMetadata metadata() {
      return new FakeMetadata(id);
    }
  }

  // named like Delta's own methods, which are called by reflection
  @SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
  public static class FakeMetadata {
    private final String id;

    FakeMetadata(String id) {
      this.id = id;
    }

    public String id() {
      return id;
    }
  }
}
