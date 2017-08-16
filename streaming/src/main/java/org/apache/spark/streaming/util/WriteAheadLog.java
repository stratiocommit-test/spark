/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.util;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * :: DeveloperApi ::
 *
 * This abstract class represents a write ahead log (aka journal) that is used by Spark Streaming
 * to save the received data (by receivers) and associated metadata to a reliable storage, so that
 * they can be recovered after driver failures. See the Spark documentation for more information
 * on how to plug in your own custom implementation of a write ahead log.
 */
@org.apache.spark.annotation.DeveloperApi
public abstract class WriteAheadLog {
  /**
   * Write the record to the log and return a record handle, which contains all the information
   * necessary to read back the written record. The time is used to the index the record,
   * such that it can be cleaned later. Note that implementations of this abstract class must
   * ensure that the written data is durable and readable (using the record handle) by the
   * time this function returns.
   */
  public abstract WriteAheadLogRecordHandle write(ByteBuffer record, long time);

  /**
   * Read a written record based on the given record handle.
   */
  public abstract ByteBuffer read(WriteAheadLogRecordHandle handle);

  /**
   * Read and return an iterator of all the records that have been written but not yet cleaned up.
   */
  public abstract Iterator<ByteBuffer> readAll();

  /**
   * Clean all the records that are older than the threshold time. It can wait for
   * the completion of the deletion.
   */
  public abstract void clean(long threshTime, boolean waitForCompletion);

  /**
   * Close this log and release any resources.
   */
  public abstract void close();
}
