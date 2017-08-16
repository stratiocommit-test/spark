/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.window

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.collection.unsafe.sort.{UnsafeExternalSorter, UnsafeSorterIterator}


/**
 * The interface of row buffer for a partition. In absence of a buffer pool (with locking), the
 * row buffer is used to materialize a partition of rows since we need to repeatedly scan these
 * rows in window function processing.
 */
private[window] abstract class RowBuffer {

  /** Number of rows. */
  def size: Int

  /** Return next row in the buffer, null if no more left. */
  def next(): InternalRow

  /** Skip the next `n` rows. */
  def skip(n: Int): Unit

  /** Return a new RowBuffer that has the same rows. */
  def copy(): RowBuffer
}

/**
 * A row buffer based on ArrayBuffer (the number of rows is limited).
 */
private[window] class ArrayRowBuffer(buffer: ArrayBuffer[UnsafeRow]) extends RowBuffer {

  private[this] var cursor: Int = -1

  /** Number of rows. */
  override def size: Int = buffer.length

  /** Return next row in the buffer, null if no more left. */
  override def next(): InternalRow = {
    cursor += 1
    if (cursor < buffer.length) {
      buffer(cursor)
    } else {
      null
    }
  }

  /** Skip the next `n` rows. */
  override def skip(n: Int): Unit = {
    cursor += n
  }

  /** Return a new RowBuffer that has the same rows. */
  override def copy(): RowBuffer = {
    new ArrayRowBuffer(buffer)
  }
}

/**
 * An external buffer of rows based on UnsafeExternalSorter.
 */
private[window] class ExternalRowBuffer(sorter: UnsafeExternalSorter, numFields: Int)
  extends RowBuffer {

  private[this] val iter: UnsafeSorterIterator = sorter.getIterator

  private[this] val currentRow = new UnsafeRow(numFields)

  /** Number of rows. */
  override def size: Int = iter.getNumRecords()

  /** Return next row in the buffer, null if no more left. */
  override def next(): InternalRow = {
    if (iter.hasNext) {
      iter.loadNext()
      currentRow.pointTo(iter.getBaseObject, iter.getBaseOffset, iter.getRecordLength)
      currentRow
    } else {
      null
    }
  }

  /** Skip the next `n` rows. */
  override def skip(n: Int): Unit = {
    var i = 0
    while (i < n && iter.hasNext) {
      iter.loadNext()
      i += 1
    }
  }

  /** Return a new RowBuffer that has the same rows. */
  override def copy(): RowBuffer = {
    new ExternalRowBuffer(sorter, numFields)
  }
}
