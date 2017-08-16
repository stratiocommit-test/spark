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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Keeps the index information for a particular map output
 * as an in-memory LongBuffer.
 */
public class ShuffleIndexInformation {
  /** offsets as long buffer */
  private final LongBuffer offsets;

  public ShuffleIndexInformation(File indexFile) throws IOException {
    int size = (int)indexFile.length();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    offsets = buffer.asLongBuffer();
    DataInputStream dis = null;
    try {
      dis = new DataInputStream(new FileInputStream(indexFile));
      dis.readFully(buffer.array());
    } finally {
      if (dis != null) {
        dis.close();
      }
    }
  }

  /**
   * Get index offset for a particular reducer.
   */
  public ShuffleIndexRecord getIndex(int reduceId) {
    long offset = offsets.get(reduceId);
    long nextOffset = offsets.get(reduceId + 1);
    return new ShuffleIndexRecord(offset, nextOffset - offset);
  }
}
