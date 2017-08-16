/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.shuffle;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.google.common.io.Closeables;
import com.google.common.io.Files;

import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages some sort-shuffle data, including the creation
 * and cleanup of directories that can be read by the {@link ExternalShuffleBlockResolver}.
 */
public class TestShuffleDataContext {
  private static final Logger logger = LoggerFactory.getLogger(TestShuffleDataContext.class);

  public final String[] localDirs;
  public final int subDirsPerLocalDir;

  public TestShuffleDataContext(int numLocalDirs, int subDirsPerLocalDir) {
    this.localDirs = new String[numLocalDirs];
    this.subDirsPerLocalDir = subDirsPerLocalDir;
  }

  public void create() {
    for (int i = 0; i < localDirs.length; i ++) {
      localDirs[i] = Files.createTempDir().getAbsolutePath();

      for (int p = 0; p < subDirsPerLocalDir; p ++) {
        new File(localDirs[i], String.format("%02x", p)).mkdirs();
      }
    }
  }

  public void cleanup() {
    for (String localDir : localDirs) {
      try {
        JavaUtils.deleteRecursively(new File(localDir));
      } catch (IOException e) {
        logger.warn("Unable to cleanup localDir = " + localDir, e);
      }
    }
  }

  /** Creates reducer blocks in a sort-based data format within our local dirs. */
  public void insertSortShuffleData(int shuffleId, int mapId, byte[][] blocks) throws IOException {
    String blockId = "shuffle_" + shuffleId + "_" + mapId + "_0";

    OutputStream dataStream = null;
    DataOutputStream indexStream = null;
    boolean suppressExceptionsDuringClose = true;

    try {
      dataStream = new FileOutputStream(
        ExternalShuffleBlockResolver.getFile(localDirs, subDirsPerLocalDir, blockId + ".data"));
      indexStream = new DataOutputStream(new FileOutputStream(
        ExternalShuffleBlockResolver.getFile(localDirs, subDirsPerLocalDir, blockId + ".index")));

      long offset = 0;
      indexStream.writeLong(offset);
      for (byte[] block : blocks) {
        offset += block.length;
        dataStream.write(block);
        indexStream.writeLong(offset);
      }
      suppressExceptionsDuringClose = false;
    } finally {
      Closeables.close(dataStream, suppressExceptionsDuringClose);
      Closeables.close(indexStream, suppressExceptionsDuringClose);
    }
  }

  /**
   * Creates an ExecutorShuffleInfo object based on the given shuffle manager which targets this
   * context's directories.
   */
  public ExecutorShuffleInfo createExecutorInfo(String shuffleManager) {
    return new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager);
  }
}
