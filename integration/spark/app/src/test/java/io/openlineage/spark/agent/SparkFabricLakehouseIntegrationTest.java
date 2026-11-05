/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OneLakeDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.SymlinksDatasetFacetIdentifiers;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;

/**
 * Runs Spark with the settings a Microsoft Fabric notebook has, against a local file system that
 * answers to OneLake paths ({@link FakeOneLakeFileSystem}). The notebook's default lakehouse is
 * {@code lh_main}, which has schemas; {@code lh_other} is another lakehouse in the same workspace.
 * Catalog databases are named the way Fabric names them.
 */
@Tag("integration-test")
@Tag("delta")
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class SparkFabricLakehouseIntegrationTest {
  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final int MOCK_SERVER_PORT = 1088;
  private static final String ROOT = "/tmp/fabric_lakehouse_it";
  private static final String WS = "d1f6473b-9f7a-4352-940b-a8d74461d3fc";
  private static final String LH_MAIN = "c7a14054-60a7-4121-8fb7-252d16f687eb";
  private static final String LH_OTHER = "d8a75818-4429-41c0-bc80-81da494ec02f";
  private static final String ONELAKE = "abfss://" + WS + "@onelake.dfs.fabric.microsoft.com";
  private static final String LAKEHOUSE_NS = "fabric-lakehouse://" + WS;

  private static ClientAndServer mockServer;
  private SparkSession spark;

  @BeforeAll
  static void beforeAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  static void afterAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  @SneakyThrows
  void beforeEach() {
    FileUtils.deleteDirectory(new File(ROOT));
    MockServerUtils.clearRequests(mockServer);
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("FabricLakehouseIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.openlineage.namespace", "fabric-namespace")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            // OneLake paths, with relative paths in the default lakehouse, as in Fabric
            .config("spark.hadoop.fs.abfss.impl", FakeOneLakeFileSystem.class.getName())
            .config("spark.hadoop." + FakeOneLakeFileSystem.ROOT_CONF, ROOT)
            .config("spark.hadoop." + FakeOneLakeFileSystem.WORKING_DIR_CONF, LH_MAIN)
            .config("spark.hadoop.fs.defaultFS", ONELAKE + "/")
            // a folder created for each session, as in Fabric
            .config("spark.sql.warehouse.dir", "file:" + ROOT + "/session-warehouse")
            // Fabric sets these in every notebook
            .config("trident.workspace.id", WS)
            .config("trident.workspace.name", "test-ws")
            .config("trident.lakehouse.id", LH_MAIN)
            .config("trident.lakehouse.name", "lh_main")
            .config("trident.catalog.metastore.workspaceId", WS)
            .config("trident.schema.name", "dbo")
            .getOrCreate();
    spark.sql(
        "CREATE DATABASE "
            + catalogDatabase("lh_main", "sales")
            + " LOCATION '"
            + ONELAKE
            + "/"
            + LH_MAIN
            + "/Tables/sales'");
    spark.sql(
        "CREATE DATABASE "
            + catalogDatabase("lh_other", "dbo")
            + " LOCATION '"
            + ONELAKE
            + "/"
            + LH_OTHER
            + "/Tables'");
    data().write().format("delta").save(ONELAKE + "/" + LH_MAIN + "/Tables/sales/orders");
    spark.sql(
        "CREATE TABLE "
            + catalogDatabase("lh_main", "sales")
            + ".orders USING delta LOCATION '"
            + ONELAKE
            + "/"
            + LH_MAIN
            + "/Tables/sales/orders'");
    clearEvents();
  }

  @AfterEach
  void afterEach() {
    spark.stop();
  }

  @Test
  void relativePathWriteIsNamedInTheDefaultLakehouse() {
    spark
        .read()
        .format("delta")
        .load("Tables/sales/orders")
        .write()
        .mode("overwrite")
        .format("delta")
        .save("Tables/sales/copy");
    List<RunEvent> events = events("execute_save_into_data_source_command");

    assertThat(inputs(events))
        .allSatisfy(
            input -> assertLakehouseTable(input, "lh_main.sales.orders", LH_MAIN, "sales/orders"));
    assertThat(outputs(events))
        .isNotEmpty()
        .allSatisfy(
            output -> assertLakehouseTable(output, "lh_main.sales.copy", LH_MAIN, "sales/copy"));
    // the read table's Delta snapshot is already loaded, so its Delta table ID is known
    assertThat(inputs(events).stream().map(i -> i.getFacets().getOneLake().getTableId()))
        .isNotEmpty()
        .allMatch(id -> id != null && id.matches("[0-9a-f-]{36}"));
  }

  @Test
  void catalogWriteToAnotherLakehouse() {
    spark
        .table(catalogDatabase("lh_main", "sales") + ".orders")
        .write()
        .mode("overwrite")
        .format("delta")
        .saveAsTable(catalogDatabase("lh_other", "dbo") + ".copy");
    List<RunEvent> events = events();

    assertThat(inputs(events))
        .allSatisfy(
            input -> assertLakehouseTable(input, "lh_main.sales.orders", LH_MAIN, "sales/orders"));
    assertThat(outputs(events))
        .isNotEmpty()
        .allSatisfy(output -> assertLakehouseTable(output, "lh_other.dbo.copy", LH_OTHER, "copy"));
  }

  @Test
  void ctasHasNoSessionFolderSymlink() {
    spark.sql(
        "CREATE TABLE "
            + catalogDatabase("lh_main", "sales")
            + ".ctas USING delta AS SELECT * FROM "
            + catalogDatabase("lh_main", "sales")
            + ".orders");
    List<OutputDataset> outputs = outputs(events());

    assertThat(outputs)
        .isNotEmpty()
        .allSatisfy(
            output -> assertLakehouseTable(output, "lh_main.sales.ctas", LH_MAIN, "sales/ctas"));
  }

  @Test
  void mergeIntoIsNamedLikeOtherWrites() {
    String table = catalogDatabase("lh_main", "sales") + ".orders";
    spark.sql(
        "MERGE INTO "
            + table
            + " t USING (SELECT 3 AS id, 'c' AS v) s ON t.id = s.id"
            + " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *");
    List<OutputDataset> outputs = outputs(events());

    assertThat(outputs)
        .isNotEmpty()
        .allSatisfy(
            output ->
                assertLakehouseTable(output, "lh_main.sales.orders", LH_MAIN, "sales/orders"));
  }

  @Test
  void pathWriteToAnotherLakehouseKeepsItsPath() {
    spark
        .read()
        .format("delta")
        .load(ONELAKE + "/" + LH_MAIN + "/Tables/sales/orders")
        .write()
        .mode("overwrite")
        .format("delta")
        .save(ONELAKE + "/" + LH_OTHER.toUpperCase(Locale.ROOT) + "/Tables/by_path");
    List<OutputDataset> outputs = outputs(events("execute_save_into_data_source_command"));

    // a Delta write shows the item is a lakehouse, but nothing names it
    assertThat(outputs)
        .isNotEmpty()
        .allSatisfy(
            output -> {
              assertThat(output.getNamespace()).isEqualTo(ONELAKE);
              assertThat(output.getName()).isEqualTo(LH_OTHER + "/Tables/by_path");
              OneLakeDatasetFacet oneLake = output.getFacets().getOneLake();
              assertThat(oneLake.getItemType()).isEqualTo("Lakehouse");
              assertThat(oneLake.getItemId()).isEqualTo(LH_OTHER);
              assertThat(oneLake.getItemName()).isNull();
              assertThat(oneLake.getWorkspaceName()).isEqualTo("test-ws");
            });
  }

  @Test
  void catalogReadOfAnotherItemNamesItWithoutAType() {
    spark.range(3).write().format("delta").saveAsTable(catalogDatabase("lh_other", "dbo") + ".src");
    clearEvents();

    spark
        .table(catalogDatabase("lh_other", "dbo") + ".src")
        .write()
        .mode("overwrite")
        .format("delta")
        .save("Tables/sales/from_other");
    List<InputDataset> inputs = inputs(events());

    assertThat(inputs)
        .isNotEmpty()
        .allSatisfy(
            input -> {
              assertThat(input.getNamespace()).isEqualTo(ONELAKE);
              assertThat(input.getName()).isEqualTo(LH_OTHER + "/Tables/src");
              OneLakeDatasetFacet oneLake = input.getFacets().getOneLake();
              assertThat(oneLake.getItemName()).isEqualTo("lh_other");
              assertThat(oneLake.getItemType()).isNull();
            });
  }

  private void assertLakehouseTable(Dataset dataset, String name, String itemId, String path) {
    String location = itemId + "/Tables/" + path;
    assertThat(dataset.getNamespace()).isEqualTo(LAKEHOUSE_NS);
    assertThat(dataset.getName()).isEqualTo(name);
    assertThat(dataset.getFacets().getSymlinks().getIdentifiers())
        .extracting(
            SymlinksDatasetFacetIdentifiers::getNamespace,
            SymlinksDatasetFacetIdentifiers::getName,
            SymlinksDatasetFacetIdentifiers::getType)
        .containsExactly(Tuple.tuple(ONELAKE, location, "LOCATION"));
    OneLakeDatasetFacet oneLake = dataset.getFacets().getOneLake();
    assertThat(oneLake.getItemType()).isEqualTo("Lakehouse");
    assertThat(oneLake.getItemId()).isEqualTo(itemId);
    assertThat(oneLake.getWorkspaceId()).isEqualTo(WS);
    assertThat(oneLake.getWorkspaceName()).isEqualTo("test-ws");
    assertThat(oneLake.getLocation()).isEqualTo(ONELAKE + "/" + location);
  }

  private org.apache.spark.sql.Dataset<Row> data() {
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType).add("v", DataTypes.StringType);
    return spark.createDataFrame(
        Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(2, "b")), schema);
  }

  // Fabric names the catalog database of a schema in a lakehouse with schemas like this
  private static String catalogDatabase(String lakehouse, String schema) {
    String name = "default%test-ws%" + lakehouse + "%" + schema;
    return base32Hex(name.getBytes(StandardCharsets.UTF_8));
  }

  private static String base32Hex(byte[] bytes) {
    String alphabet = "0123456789abcdefghijklmnopqrstuv";
    StringBuilder result = new StringBuilder();
    int buffer = 0;
    int bits = 0;
    for (byte b : bytes) {
      buffer = (buffer << 8) | (b & 0xff);
      bits += 8;
      while (bits >= 5) {
        result.append(alphabet.charAt((buffer >> (bits - 5)) & 0x1f));
        bits -= 5;
      }
    }
    if (bits > 0) {
      result.append(alphabet.charAt((buffer << (5 - bits)) & 0x1f));
    }
    return result.toString();
  }

  // drops the events of the setup, once they have all been sent
  @SneakyThrows
  private void clearEvents() {
    Thread.sleep(3000);
    MockServerUtils.clearRequests(mockServer);
  }

  @SneakyThrows
  private List<RunEvent> events() {
    Thread.sleep(3000);
    return getEventsEmitted(mockServer);
  }

  // the events of the job that runs the command, leaving out Delta's own jobs
  private List<RunEvent> events(String command) {
    return events().stream()
        .filter(e -> e.getJob().getName().contains(command))
        .collect(Collectors.toList());
  }

  private static List<InputDataset> inputs(List<RunEvent> events) {
    return events.stream().flatMap(e -> e.getInputs().stream()).collect(Collectors.toList());
  }

  private static List<OutputDataset> outputs(List<RunEvent> events) {
    return events.stream()
        .flatMap(e -> Stream.of(e.getOutputs()).flatMap(List::stream))
        .collect(Collectors.toList());
  }
}
