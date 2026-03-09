/** 
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.synthetic

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.StructType

/**
 * Fake FileIndex for use by the harness plan generator.
 *
 * Generates synthetic file paths without any filesystem interaction. Used with
 * LogicalRelation nodes so that OL visitors can call inputFiles() and sizeInBytes()
 * without touching the real filesystem or a Hive metastore.
 *
 * OL accesses:
 *  - inputFiles()   — emitted as dataset facet source files
 *  - sizeInBytes()  — used for partition size estimates
 *  - rootPaths()    — used by some builders for dataset name derivation
 *
 * @param fileCount  number of synthetic file paths to generate
 * @param rootPath   root URI embedded in all generated paths and returned by rootPaths()
 */
class SyntheticFileIndex(
    fileCount: Int,
    rootPath: String = "hdfs://synthetic-cluster/data") extends FileIndex {

  private val _inputFiles: Array[String] =
    (0 until fileCount).map(i => s"$rootPath/part-$i.parquet").toArray

  override def rootPaths: Seq[Path] = Seq(new Path(rootPath))

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = Seq.empty

  override def inputFiles: Array[String] = _inputFiles

  override def refresh(): Unit = ()

  /** 128 MB per synthetic file — a realistic estimate for Parquet on HDFS. */
  override def sizeInBytes: Long = fileCount.toLong * 128L * 1024L * 1024L

  override def partitionSchema: StructType = StructType(Seq.empty)
}
