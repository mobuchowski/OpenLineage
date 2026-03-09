/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.harness.synthetic;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Synthetic Hadoop FileSystem for benchmark use. Registered as the {@code hdfs} scheme
 * implementation via {@code spark.hadoop.fs.hdfs.impl} so that the harness never attempts a real
 * HDFS connection to {@code synthetic-cluster}.
 *
 * <p>All path-level operations (open, getFileStatus, listStatus, etc.) throw {@link IOException}
 * or return safe defaults. The only contract that matters for OL lineage extraction is:
 * <ul>
 *   <li>{@link #getFileStatus(Path)} throws {@link FileNotFoundException} — causes
 *       {@code isSingleFileRelation()} and {@code getDirectoryPath()} to catch IOException and
 *       continue without HDFS resolution.</li>
 *   <li>{@link #initialize(URI, Configuration)} succeeds silently — no HDFS NameNode contact.</li>
 * </ul>
 *
 * <p>Registered in {@code buildSparkSession()} via:
 * <pre>
 *   .config("spark.hadoop.fs.hdfs.impl", SyntheticHadoopFileSystem.class.getName())
 *   .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true")
 * </pre>
 */
public class SyntheticHadoopFileSystem extends FileSystem {

  private URI uri;
  private Path workingDir;

  @Override
  public String getScheme() {
    return "hdfs";
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = new Path("/");
    setConf(conf);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    // Return a directory FileStatus so that:
    //   - isSingleFileRelation() gets isFile() == false → correctly not a single file
    //   - PlanUtils.getDirectoryPath() gets isFile() == false → returns path as-is
    // Both callers treat this as a directory path, which is correct for synthetic HDFS paths.
    // Returning a status (rather than throwing) prevents OL from logging a WARN.
    return new FileStatus(0L, true, 1, 0L, 0L, f);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    throw new IOException("SyntheticHadoopFileSystem: open not supported");
  }

  @Override
  public FSDataOutputStream create(
      Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    throw new IOException("SyntheticHadoopFileSystem: create not supported");
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new IOException("SyntheticHadoopFileSystem: append not supported");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return new FileStatus[0];
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    this.workingDir = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return false;
  }
}
