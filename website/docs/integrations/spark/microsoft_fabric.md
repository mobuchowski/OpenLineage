---
sidebar_position: 9
title: Microsoft Fabric
---

OpenLineage names Fabric warehouse and lakehouse tables as the [naming conventions](../../spec/naming.md) describe.
Warehouse tables are covered first, then [lakehouse tables](#lakehouse-tables).

## Warehouse tables

When you read or write a Fabric warehouse table with Microsoft's Spark connector (`synapsesql`), OpenLineage reports
the table itself, named as the [naming conventions](../../spec/naming.md) describe:

| Namespace                                                                           | Name                           |
|:------------------------------------------------------------------------------------|:-------------------------------|
| `fabric-warehouse://{workspace SQL address}`, such as `fabric-warehouse://6obax…-ac4e….datawarehouse.fabric.microsoft.com` | `{warehouse}.{schema}.{table}` |

For example, `spark.read.synapsesql("wh_primary.raw.orders")` is reported as reading
`fabric-warehouse://ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.datawarehouse.fabric.microsoft.com` /
`wh_primary.raw.orders`.

Each Fabric workspace has one SQL address, shared by all its warehouses.

:::note
Fabric comes with its own copy of the OpenLineage Spark integration, which doesn't do this. To get this behavior, load
the `openlineage-spark` jar yourself with `spark.jars`, and set `spark.driver.userClassPathFirst=true` so that your
copy is used instead of Fabric's.
:::

### Reads

The connector reads tables through an ordinary SQL Server connection to the workspace's SQL address. OpenLineage
recognizes Fabric addresses and reports these tables as `fabric-warehouse://`, not `sqlserver://`.

The same address can be written in several ways, so OpenLineage always writes it the same way: in lowercase, without
the `.z{xx}` label that Fabric sometimes adds (for example `.z8b`), and without port 1433, the default. This way a
table always gets the same name, however the connector wrote the address.

Addresses for a workspace in another organization (guest access) have an extra part and aren't recognized. Those
tables keep the `sqlserver://` name.

### Writes

The connector writes a table in two steps:

1. Spark saves the data as Parquet files in a temporary folder.
2. The connector loads those files into the table with `COPY INTO`. This step runs outside Spark.

OpenLineage only sees the first step. It reports the table being loaded as the output, not the temporary folder.

The first step doesn't include the workspace's SQL address, so OpenLineage works it out in this order:

1. If you set the address in `spark.datawarehouse.{warehouse}.sqlendpoint`, that address is used, as long as it is the
   address of a Fabric workspace. If it isn't, for example because it is an address for a workspace in another
   organization, the write is reported without an output.
2. Otherwise, the address is built from two IDs: the tenant ID, which Fabric provides in `trident.tenant.id`, and the
   workspace ID. The workspace ID comes from the connector's `WorkspaceId` option, or from `trident.workspace.id` if
   you don't set the option. When you write to a warehouse in a different workspace from your notebook, set
   `WorkspaceId`; otherwise the write is reported against the notebook's workspace.

If neither works, the write is reported without an output.

#### How the address is built

The address is `{tenant part}-{workspace part}.datawarehouse.fabric.microsoft.com`. Each part is 26 characters made
from an ID: the ID's 16 bytes, in the order Microsoft's .NET stores them, written in lowercase base32 without `=`
padding.

Microsoft doesn't document this. It matches the addresses of real workspaces, and OpenLineage's tests check it against
known pairs of IDs and addresses. If Microsoft ever changes it, set the address yourself with
`spark.datawarehouse.{warehouse}.sqlendpoint`, as described above, to keep the names correct.

#### Facets on written tables

Two facets differ from other file writes:

- **No `lifecycleStateChange`.** The temporary folder is always overwritten, whichever save mode you choose
  (append, overwrite and so on), and the mode you chose isn't visible to OpenLineage. Rather than report a possibly
  wrong mode, OpenLineage leaves this facet out.
- **`outputStatistics` describes the temporary files.** The row count matches the rows loaded into the table, but the
  byte size and file count are those of the temporary Parquet files.

### OneLake file location

If you pass the warehouse's ID with the connector's `DatawarehouseId` option, on a read or a write, OpenLineage also
reports where the table's files are stored in OneLake, as a `LOCATION` symlink:

```python
from com.microsoft.spark.fabric.Constants import Constants

df = (
    spark.read
    .option(Constants.WorkspaceId, "d1f6473b-9f7a-4352-940b-a8d74461d3fc")
    .option(Constants.DatawarehouseId, "d75873bd-716b-4378-a37e-864c76fc9b3b")
    .synapsesql("wh_primary.raw.orders")
)
```

This read gets the symlink `abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com` /
`d75873bd-716b-4378-a37e-864c76fc9b3b/Tables/raw/orders`.

The location always uses IDs, because OneLake refuses paths that mix IDs and names. Without `DatawarehouseId`, the
connector doesn't make the warehouse's ID available, so no location is reported.

### The `oneLake` facet

Every Fabric table that OpenLineage reports, read or written, also gets the
[`oneLake` facet](../../spec/facets/dataset-facets/onelake.md). It describes the table in OneLake terms, so you can
link it to the warehouse that holds it:

| Field           | Value                                                                                                   |
|:----------------|:--------------------------------------------------------------------------------------------------------|
| `itemType`      | `Warehouse`                                                                                             |
| `itemName`      | the warehouse, the first part of the table's name                                                       |
| `workspaceId`   | the workspace ID, read from the SQL address                                                             |
| `workspaceName` | the workspace's name, from Fabric's `trident.workspace.name`, only for the workspace the notebook runs in |
| `itemId`        | the warehouse ID, only when the `LOCATION` symlink is reported (see above)                              |
| `location`      | the same OneLake location as the `LOCATION` symlink, as a single path                                   |

For the read above, the facet is:

```json
"oneLake": {
  "location": "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com/d75873bd-716b-4378-a37e-864c76fc9b3b/Tables/raw/orders",
  "workspaceId": "d1f6473b-9f7a-4352-940b-a8d74461d3fc",
  "workspaceName": "do-test-env-fabric",
  "itemId": "d75873bd-716b-4378-a37e-864c76fc9b3b",
  "itemName": "wh_primary",
  "itemType": "Warehouse"
}
```

Everything comes from the table's own name and options. The workspace ID is never taken from the notebook's
settings, because a notebook can read and write warehouses in other workspaces. Outside Fabric, for example when you
read a warehouse over JDBC from your own Spark cluster, the facet has the same fields except `workspaceName`.

To leave the facet out, add `oneLake` to the disabled facets: `spark.openlineage.facets.oneLake.disabled=true`.

## Lakehouse tables

Spark reads and writes lakehouse tables through their files in OneLake, so it often sees only a path, such as
`abfss://d1f6473b-…@onelake.dfs.fabric.microsoft.com/c7a14054-…/Tables/sales/orders`. That path doesn't say whether
the item is a lakehouse or a warehouse, and often doesn't name it. OpenLineage names a table as a lakehouse table only
when it knows the workspace ID, the lakehouse's name, and that the item is a lakehouse:

| Namespace                           | Name                           |
|:------------------------------------|:-------------------------------|
| `fabric-lakehouse://{workspace ID}` | `{lakehouse}.{schema}.{table}` |

Lakehouses without schemas use the schema `dbo`. The table's OneLake location, written with IDs, is added as a
`LOCATION` symlink when the lakehouse's ID is known, and everything else known goes into the `oneLake` facet.

OpenLineage knows that an item is a lakehouse, and what it is called, in these cases:

1. **Writes.** Spark can write Delta tables under `Tables/` only in lakehouses; a warehouse rejects the write. So a
   Delta table written under an item's `Tables/` folder is in a lakehouse. Its name comes from one of the next cases;
   without one, the facet still says `itemType: Lakehouse`.
2. **The notebook's default lakehouse.** In Fabric, a table in the item whose ID is the default lakehouse's
   (`trident.lakehouse.id`) is named with `trident.lakehouse.name`.
3. **Catalog tables.** Fabric's Spark catalog names the item and schema of every table, so tables written with
   `saveAsTable`, `CREATE TABLE … AS SELECT`, `INSERT INTO` or `MERGE INTO` are named from it. The catalog also lists
   warehouses, so a table you only read through the catalog is a lakehouse table only in the default lakehouse; for
   other items, the facet has `itemName` but no `itemType`.
4. **Paths with names.** A path such as `abfss://my-ws@onelake.dfs.fabric.microsoft.com/lh_sales.Lakehouse/Tables/…`
   is named as a lakehouse table when the workspace is the notebook's (`trident.workspace.name`), so that its ID is
   known.

For example, in a notebook whose default lakehouse is `lh_shopist`:

| Spark code                                                 | Reported as                                                                 |
|:-----------------------------------------------------------|:----------------------------------------------------------------------------|
| `spark.table("sales.orders")`                              | `fabric-lakehouse://d1f6473b-…` / `lh_shopist.sales.orders`                 |
| `df.write.format("delta").save("Tables/sales/copy")`       | `fabric-lakehouse://d1f6473b-…` / `lh_shopist.sales.copy`                   |
| `df.write.saveAsTable("lh_other.orders")`                  | `fabric-lakehouse://d1f6473b-…` / `lh_other.dbo.orders`                     |
| `spark.table("wh_primary.raw.orders")`                     | `abfss://d1f6473b-…@onelake.dfs.fabric.microsoft.com` / `d75873bd-…/Tables/raw/orders`, with `itemName: wh_primary` |
| `spark.read.load("abfss://…/d8a75818-…/Tables/t")`         | `abfss://d1f6473b-…@onelake.dfs.fabric.microsoft.com` / `d8a75818-…/Tables/t` |

Any other OneLake path keeps its path as the name, written the same way every time: the
`onelake.dfs.fabric.microsoft.com` host, lowercase IDs, no trailing slash, and `.Warehouse` for paths that Spark
reports with `.Datawarehouse`. Outside Fabric, for example in your own Spark cluster reading OneLake, there are no
`trident.*` settings, so tables keep their path names, with the `oneLake` facet describing what the path says.

The workspace ID is always taken from the table's path, never from the notebook's settings, because a notebook can
read and write items in other workspaces.

When a table's Delta snapshot is already loaded, for example when it is read, the facet also has `tableId`, the
table's Delta table ID. For a table reached through a shortcut, this is the target table's ID, which links the
shortcut to the table it points to.

### Changes from earlier versions

- Writes with relative paths, such as `.save("Tables/orders")`, were reported as `file` / `Tables/orders`. They are now
  reported where they are written: in Fabric, the default lakehouse.
- Tables created with `CREATE TABLE … AS SELECT` or `saveAsTable` had a `TABLE` symlink in the folder named by
  `spark.sql.warehouse.dir`. In Fabric, that folder is created for each session, so the symlink changed every time. It
  is no longer reported in Fabric.
