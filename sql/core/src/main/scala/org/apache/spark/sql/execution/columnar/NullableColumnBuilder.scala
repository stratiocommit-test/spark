/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.columnar

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.catalyst.InternalRow

/**
 * A stackable trait used for building byte buffer for a column containing null values.  Memory
 * layout of the final byte buffer is:
 * {{{
 *    .------------------- Null count N (4 bytes)
 *    |   .--------------- Null positions (4 x N bytes, empty if null count is zero)
 *    |   |     .--------- Non-null elements
 *    V   V     V
 *   +---+-----+---------+
 *   |   | ... | ... ... |
 *   +---+-----+---------+
 * }}}
 */
private[columnar] trait NullableColumnBuilder extends ColumnBuilder {
  protected var nulls: ByteBuffer = _
  protected var nullCount: Int = _
  private var pos: Int = _

  abstract override def initialize(
      initialSize: Int,
      columnName: String,
      useCompression: Boolean): Unit = {

    nulls = ByteBuffer.allocate(1024)
    nulls.order(ByteOrder.nativeOrder())
    pos = 0
    nullCount = 0
    super.initialize(initialSize, columnName, useCompression)
  }

  abstract override def appendFrom(row: InternalRow, ordinal: Int): Unit = {
    columnStats.gatherStats(row, ordinal)
    if (row.isNullAt(ordinal)) {
      nulls = ColumnBuilder.ensureFreeSpace(nulls, 4)
      nulls.putInt(pos)
      nullCount += 1
    } else {
      super.appendFrom(row, ordinal)
    }
    pos += 1
  }

  abstract override def build(): ByteBuffer = {
    val nonNulls = super.build()
    val nullDataLen = nulls.position()

    nulls.limit(nullDataLen)
    nulls.rewind()

    val buffer = ByteBuffer
      .allocate(4 + nullDataLen + nonNulls.remaining())
      .order(ByteOrder.nativeOrder())
      .putInt(nullCount)
      .put(nulls)
      .put(nonNulls)

    buffer.rewind()
    buffer
  }

  protected def buildNonNulls(): ByteBuffer = {
    nulls.limit(nulls.position()).rewind()
    super.build()
  }
}
