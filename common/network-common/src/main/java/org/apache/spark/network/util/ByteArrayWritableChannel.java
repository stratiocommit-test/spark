/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.util;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A writable channel that stores the written data in a byte array in memory.
 */
public class ByteArrayWritableChannel implements WritableByteChannel {

  private final byte[] data;
  private int offset;

  public ByteArrayWritableChannel(int size) {
    this.data = new byte[size];
  }

  public byte[] getData() {
    return data;
  }

  public int length() {
    return offset;
  }

  /** Resets the channel so that writing to it will overwrite the existing buffer. */
  public void reset() {
    offset = 0;
  }

  /**
   * Reads from the given buffer into the internal byte array.
   */
  @Override
  public int write(ByteBuffer src) {
    int toTransfer = Math.min(src.remaining(), data.length - offset);
    src.get(data, offset, toTransfer);
    offset += toTransfer;
    return toTransfer;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean isOpen() {
    return true;
  }

}
