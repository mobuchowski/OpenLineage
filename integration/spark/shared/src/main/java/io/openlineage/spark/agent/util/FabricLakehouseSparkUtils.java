/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.FabricLakehouseUtils;
import io.openlineage.client.utils.FabricWarehouseUtils;
import io.openlineage.client.utils.OneLakeLocation;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Names Microsoft Fabric lakehouse tables that Spark reads and writes.
 *
 * <p>Spark only sees where a table's files are in OneLake, for example {@code
 * abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/Tables/sales/orders}. That path
 * doesn't say whether the item is a lakehouse or a warehouse, and usually doesn't name it. A table
 * is named {@code fabric-lakehouse://<workspace ID>} / {@code <lakehouse>.<schema>.<table>} only
 * when the workspace ID, the lakehouse's name and the fact that the item is a lakehouse are all
 * known. That is the case when:
 *
 * <ol>
 *   <li><b>(a)</b> Spark writes a Delta table under an item's {@code Tables/} folder: only
 *       lakehouses accept Spark writes there, warehouses reject them. The lakehouse's name still
 *       has to come from one of the other rules or from the Spark catalog.
 *   <li><b>(b)</b> in Fabric, the item's ID is the session's default lakehouse ({@code
 *       trident.lakehouse.id}); its name is {@code trident.lakehouse.name}.
 *   <li><b>(c)</b> the table comes from Fabric's Spark catalog, which names the item and the
 *       schema. The catalog lists warehouses too, so on its own this only names the item; the item
 *       is a lakehouse only when rule (a) or (b) also applies.
 *   <li><b>(d)</b> the path names the item as {@code <name>.Lakehouse}, and the workspace's ID is
 *       known because the path's workspace name is the session's ({@code trident.workspace.name}).
 * </ol>
 *
 * Any other OneLake path keeps its path as the name, written the standard way (see {@link
 * OneLakeLocation#canonicalIdentifier()}), and everything known goes into the {@code oneLake}
 * facet. The workspace ID is never taken from the session when the path names another workspace.
 *
 * <p>The Spark catalog's table identifiers are only available where the dataset is first
 * identified, while whether it is read or written is only known when the dataset is built. {@link
 * #withCatalogTable} therefore records the catalog's name as an internal symlink, which {@link
 * #resolve} always removes.
 */
@Slf4j
public final class FabricLakehouseSparkUtils {
  public static final String LAKEHOUSE_ID_CONF = "trident.lakehouse.id";
  public static final String LAKEHOUSE_NAME_CONF = "trident.lakehouse.name";
  public static final String LAKEHOUSE_WORKSPACE_ID_CONF = "trident.catalog.metastore.workspaceId";

  /**
   * Set only in sessions whose default lakehouse has schemas. In other sessions, Fabric's Spark
   * catalog names databases after lakehouses.
   */
  public static final String SCHEMA_NAME_CONF = "trident.schema.name";

  static final String CATALOG_HINT_NAMESPACE = "openlineage-internal://fabric-catalog";

  // Fabric names catalog databases of schema-enabled lakehouses with the lowercase, unpadded
  // base32hex encoding of "default%<workspace>%<item>%<schema>". Microsoft doesn't document it.
  private static final String CATALOG_PREFIX = "default";
  private static final String CATALOG_SEPARATOR = "%";
  private static final int CATALOG_PARTS = 4;
  private static final String BASE32HEX_ALPHABET = "0123456789abcdefghijklmnopqrstuv";
  private static final String HINT_SEPARATOR = "/";
  private static final int HINT_PARTS = 5;
  private static final String UTF_8 = "UTF-8";

  private FabricLakehouseSparkUtils() {}

  /** What a Fabric Spark session knows about itself, from its {@code trident.*} settings. */
  @Value
  static class FabricSession {
    UUID workspaceId;
    String workspaceName;
    UUID lakehouseId;
    String lakehouseName;
    UUID lakehouseWorkspaceId;
    boolean lakehouseHasSchemas;

    static Optional<FabricSession> of(Function<String, Optional<String>> conf) {
      Optional<UUID> workspaceId =
          conf.apply(FabricWarehouseSparkUtils.WORKSPACE_ID_CONF)
              .flatMap(FabricWarehouseUtils::parseGuid);
      Optional<UUID> lakehouseId =
          conf.apply(LAKEHOUSE_ID_CONF).flatMap(FabricWarehouseUtils::parseGuid);
      if (!workspaceId.isPresent() && !lakehouseId.isPresent()) {
        return Optional.empty();
      }
      return Optional.of(
          new FabricSession(
              workspaceId.orElse(null),
              nonEmpty(conf.apply(OneLakeFacetUtils.WORKSPACE_NAME_CONF)).orElse(null),
              lakehouseId.orElse(null),
              nonEmpty(conf.apply(LAKEHOUSE_NAME_CONF)).orElse(null),
              conf.apply(LAKEHOUSE_WORKSPACE_ID_CONF)
                  .flatMap(FabricWarehouseUtils::parseGuid)
                  .orElse(workspaceId.orElse(null)),
              nonEmpty(conf.apply(SCHEMA_NAME_CONF)).isPresent()));
    }
  }

  /** A table's name in Fabric's Spark catalog. The workspace's name is unknown for some tables. */
  @Value
  static class CatalogName {
    String workspaceName;
    String item;
    String schema;
    String table;
    boolean delta;
  }

  /** The dataset's final identifier, and what is known about it for the {@code oneLake} facet. */
  @Value
  public static class Resolved {
    DatasetIdentifier identifier;
    Optional<OneLakeLocation> oneLake;
  }

  /** Whether the Spark session runs in Microsoft Fabric. */
  public static boolean isFabric(Function<String, Optional<String>> conf) {
    return FabricSession.of(conf).isPresent();
  }

  /**
   * Records a table's Spark catalog name on its identifier, for {@link #resolve}. This does nothing
   * outside Fabric, or when the catalog's database name can't be read.
   *
   * @param identifier the table's identifier, named after its location
   * @param conf looks up a Spark setting
   * @param database the catalog database
   * @param table the catalog table
   * @param delta whether the table is a Delta table
   * @return the same identifier
   */
  public static DatasetIdentifier withCatalogTable(
      DatasetIdentifier identifier,
      Function<String, Optional<String>> conf,
      Optional<String> database,
      String table,
      boolean delta) {
    Optional<FabricSession> session = FabricSession.of(conf);
    if (!session.isPresent() || !database.isPresent() || table == null) {
      return identifier;
    }
    Optional<CatalogName> name = catalogName(database.get(), table, delta, session.get());
    if (name.isPresent()) {
      identifier.withSymlink(
          encodeHint(name.get()), CATALOG_HINT_NAMESPACE, DatasetIdentifier.SymlinkType.TABLE);
    } else {
      log.debug("Cannot read the Fabric catalog database name {}", database.get());
    }
    return identifier;
  }

  /**
   * Gives a dataset its final name: a {@code fabric-lakehouse} table when the rules in this class's
   * description allow it, otherwise its OneLake path written the standard way. Datasets that aren't
   * in OneLake are returned unchanged, without the internal catalog symlink.
   *
   * @param identifier the dataset's identifier
   * @param output whether the dataset is written
   * @param deltaWrite whether the dataset is written as a Delta table
   * @param conf looks up a Spark setting
   */
  public static Resolved resolve(
      DatasetIdentifier identifier,
      boolean output,
      boolean deltaWrite,
      Function<String, Optional<String>> conf) {
    List<DatasetIdentifier.Symlink> given =
        identifier.getSymlinks() == null ? Collections.emptyList() : identifier.getSymlinks();
    List<DatasetIdentifier.Symlink> symlinks = new ArrayList<>();
    Optional<CatalogName> hint = Optional.empty();
    for (DatasetIdentifier.Symlink symlink : given) {
      if (CATALOG_HINT_NAMESPACE.equals(symlink.getNamespace())) {
        hint = decodeHint(symlink.getName());
      } else {
        symlinks.add(symlink);
      }
    }
    Optional<OneLakeLocation> parsed =
        OneLakeLocation.parse(identifier.getNamespace(), identifier.getName());
    if (!parsed.isPresent()) {
      return new Resolved(
          symlinks.size() == given.size()
              ? identifier
              : new DatasetIdentifier(identifier.getName(), identifier.getNamespace(), symlinks),
          Optional.empty());
    }

    OneLakeLocation location =
        describe(parsed.get(), output, deltaWrite, hint, FabricSession.of(conf));
    DatasetIdentifier named =
        lakehouseName(location)
            .orElseGet(
                () ->
                    location
                        .canonicalIdentifier()
                        .orElse(
                            new DatasetIdentifier(
                                identifier.getName(), identifier.getNamespace())));
    symlinks.forEach(named::withSymlink);
    return new Resolved(named, Optional.of(location));
  }

  /** The same as {@link #resolve} for a dataset that is read. */
  public static DatasetIdentifier resolveInput(
      DatasetIdentifier identifier, Function<String, Optional<String>> conf) {
    return resolve(identifier, false, false, conf).getIdentifier();
  }

  static OneLakeLocation describe(
      OneLakeLocation parsed,
      boolean output,
      boolean deltaWrite,
      Optional<CatalogName> hint,
      Optional<FabricSession> session) {
    OneLakeLocation.OneLakeLocationBuilder builder = parsed.toBuilder();
    UUID workspaceId = parsed.getWorkspaceId();
    String itemName = parsed.getItemName();
    String itemType = parsed.getItemType();

    if (session.isPresent()) {
      FabricSession fabric = session.get();
      // a path that names the session's workspace: its ID is known (rule d needs it)
      if (workspaceId == null
          && fabric.getWorkspaceId() != null
          && parsed.getWorkspaceName() != null
          && parsed.getWorkspaceName().equals(fabric.getWorkspaceName())) {
        workspaceId = fabric.getWorkspaceId();
        builder.workspaceId(workspaceId);
      }
      // rule (b): the session's default lakehouse, by ID
      if (parsed.getItemId() != null && parsed.getItemId().equals(fabric.getLakehouseId())) {
        itemName = fabric.getLakehouseName();
        itemType = OneLakeLocation.LAKEHOUSE;
        builder.itemName(itemName).itemType(itemType);
      }
      // the session's default lakehouse, by name, in the workspace that holds it
      if (parsed.getItemId() == null
          && OneLakeLocation.LAKEHOUSE.equals(itemType)
          && itemName != null
          && itemName.equals(fabric.getLakehouseName())
          && workspaceId != null
          && workspaceId.equals(fabric.getLakehouseWorkspaceId())) {
        builder.itemId(fabric.getLakehouseId());
      }
    }

    // rule (c): the catalog names the item, but doesn't say whether it is a lakehouse
    boolean delta = deltaWrite;
    if (hint.isPresent()) {
      delta = delta || hint.get().isDelta();
      if (itemName == null) {
        itemName = hint.get().getItem();
        builder.itemName(itemName);
      }
      if (parsed.getWorkspaceName() == null && hint.get().getWorkspaceName() != null) {
        builder.workspaceName(hint.get().getWorkspaceName());
      }
    }

    // rule (a): Spark can only write Delta tables under a lakehouse's Tables/ folder
    if (output && delta && itemType == null && parsed.tableParts().isPresent()) {
      builder.itemType(OneLakeLocation.LAKEHOUSE);
    }
    return builder.build();
  }

  private static Optional<DatasetIdentifier> lakehouseName(OneLakeLocation location) {
    if (!OneLakeLocation.LAKEHOUSE.equals(location.getItemType())) {
      return Optional.empty();
    }
    return FabricLakehouseUtils.fromOneLakeLocation(location);
  }

  static Optional<CatalogName> catalogName(
      String database, String table, boolean delta, FabricSession session) {
    Optional<String> decoded = decodeBase32Hex(database);
    if (decoded.isPresent()) {
      String[] parts = decoded.get().split(CATALOG_SEPARATOR, -1);
      if (parts.length == CATALOG_PARTS
          && CATALOG_PREFIX.equals(parts[0])
          && !parts[1].isEmpty()
          && !parts[2].isEmpty()
          && !parts[3].isEmpty()) {
        return Optional.of(new CatalogName(parts[1], parts[2], parts[3], table, delta));
      }
    }
    if (!session.isLakehouseHasSchemas() && !database.isEmpty()) {
      // the default lakehouse has no schemas: databases are named after lakehouses
      return Optional.of(
          new CatalogName(null, database, FabricLakehouseUtils.DEFAULT_SCHEMA, table, delta));
    }
    return Optional.empty();
  }

  /**
   * Decodes lowercase, unpadded base32hex, or returns empty if the text isn't valid UTF-8 in it.
   */
  static Optional<String> decodeBase32Hex(String text) {
    if (text == null || text.isEmpty()) {
      return Optional.empty();
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    int buffer = 0;
    int bits = 0;
    for (char c : text.toLowerCase(Locale.ROOT).toCharArray()) {
      int value = BASE32HEX_ALPHABET.indexOf(c);
      if (value < 0) {
        return Optional.empty();
      }
      buffer = (buffer << 5) | value;
      bits += 5;
      if (bits >= Byte.SIZE) {
        bytes.write((buffer >> (bits - Byte.SIZE)) & 0xff);
        bits -= Byte.SIZE;
      }
    }
    // leftover bits are padding and must be zero
    if ((buffer & ((1 << bits) - 1)) != 0) {
      return Optional.empty();
    }
    byte[] decoded = bytes.toByteArray();
    String result = new String(decoded, StandardCharsets.UTF_8);
    if (!Arrays.equals(result.getBytes(StandardCharsets.UTF_8), decoded)) {
      return Optional.empty();
    }
    return Optional.of(result);
  }

  private static String encodeHint(CatalogName name) {
    return String.join(
        HINT_SEPARATOR,
        encode(name.getWorkspaceName()),
        encode(name.getItem()),
        encode(name.getSchema()),
        encode(name.getTable()),
        name.isDelta() ? "delta" : "");
  }

  private static Optional<CatalogName> decodeHint(String hint) {
    String[] parts = hint.split(HINT_SEPARATOR, -1);
    if (parts.length != HINT_PARTS) {
      return Optional.empty();
    }
    return Optional.of(
        new CatalogName(
            decode(parts[0]),
            decode(parts[1]),
            decode(parts[2]),
            decode(parts[3]),
            "delta".equals(parts[4])));
  }

  private static String encode(String value) {
    if (value == null) {
      return "";
    }
    try {
      return URLEncoder.encode(value, UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String decode(String value) {
    if (value.isEmpty()) {
      return null;
    }
    try {
      return URLDecoder.decode(value, UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Optional<String> nonEmpty(Optional<String> value) {
    return value.filter(v -> !v.trim().isEmpty());
  }
}
