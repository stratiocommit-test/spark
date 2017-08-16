/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources

import java.io.Closeable

import org.apache.hadoop.mapreduce.RecordReader

import org.apache.spark.sql.catalyst.InternalRow

/**
 * An adaptor from a Hadoop [[RecordReader]] to an [[Iterator]] over the values returned.
 *
 * Note that this returns [[Object]]s instead of [[InternalRow]] because we rely on erasure to pass
 * column batches by pretending they are rows.
 */
class RecordReaderIterator[T](
    private[this] var rowReader: RecordReader[_, T]) extends Iterator[T] with Closeable {
  private[this] var havePair = false
  private[this] var finished = false

  override def hasNext: Boolean = {
    if (!finished && !havePair) {
      finished = !rowReader.nextKeyValue
      if (finished) {
        // Close and release the reader here; close() will also be called when the task
        // completes, but for tasks that read from many files, it helps to release the
        // resources early.
        close()
      }
      havePair = !finished
    }
    !finished
  }

  override def next(): T = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    havePair = false
    rowReader.getCurrentValue
  }

  override def close(): Unit = {
    if (rowReader != null) {
      try {
        // Close the reader and release it. Note: it's very important that we don't close the
        // reader more than once, since that exposes us to MAPREDUCE-5918 when running against
        // older Hadoop 2.x releases. That bug can lead to non-deterministic corruption issues
        // when reading compressed input.
        rowReader.close()
      } finally {
        rowReader = null
      }
    }
  }
}
