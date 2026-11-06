/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import io.openlineage.client.dataset.partition.trimmer.DatasetNameTrimmer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

@Value
public class DatasetIdentifier {
  String name;
  String namespace;
  List<Symlink> symlinks;

  /**
   * The table's name in the Spark catalog it was looked up in, if any. Integrations use it to name
   * the dataset; it isn't reported, and doesn't take part in equality.
   */
  @EqualsAndHashCode.Exclude @ToString.Exclude CatalogTableName catalogTableName;

  public enum SymlinkType {
    TABLE,
    LOCATION
  };

  public DatasetIdentifier(String name, String namespace) {
    this(name, namespace, new LinkedList<>());
  }

  public DatasetIdentifier(String name, String namespace, List<Symlink> symlinks) {
    this(name, namespace, symlinks, null);
  }

  private DatasetIdentifier(
      String name, String namespace, List<Symlink> symlinks, CatalogTableName catalogTableName) {
    this.name = name;
    this.namespace = namespace;
    this.symlinks = symlinks;
    this.catalogTableName = catalogTableName;
  }

  /** The same dataset, with the table's name in the Spark catalog it was looked up in. */
  public DatasetIdentifier withCatalogTableName(CatalogTableName catalogTableName) {
    return new DatasetIdentifier(name, namespace, symlinks, catalogTableName);
  }

  public DatasetIdentifier withSymlink(String name, String namespace, SymlinkType type) {
    symlinks.add(new Symlink(name, namespace, type));
    return this;
  }

  public DatasetIdentifier withSymlink(Symlink symlink) {
    symlinks.add(symlink);
    return this;
  }

  public DatasetIdentifier withTrimmedName(Collection<DatasetNameTrimmer> trimmers) {
    String trimmedName = name;
    boolean continueTrimming = true;

    while (continueTrimming) {
      continueTrimming = false;
      for (DatasetNameTrimmer trimmer : trimmers) {
        if (trimmer.canTrim(trimmedName)) {
          trimmedName = trimmer.trim(trimmedName);
          continueTrimming = true;
        }
      }
    }

    if (name.equals(trimmedName)) {
      return this;
    } else {
      return new DatasetIdentifier(trimmedName, namespace, symlinks, catalogTableName);
    }
  }

  /** A table's name in a Spark catalog. */
  @Value
  public static class CatalogTableName {
    /** The catalog database, or {@code null} if there is none. */
    String database;

    String table;

    /** The table's provider, such as {@code delta}, or {@code null} if unknown. */
    String provider;
  }

  @Value
  public static class Symlink {
    String name;
    String namespace;
    SymlinkType type;
  }
}
