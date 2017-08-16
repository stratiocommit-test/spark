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

private[columnar] trait NullableColumnAccessor extends ColumnAccessor {
  private var nullsBuffer: ByteBuffer = _
  private var nullCount: Int = _
  private var seenNulls: Int = 0

  private var nextNullIndex: Int = _
  private var pos: Int = 0

  abstract override protected def initialize(): Unit = {
    nullsBuffer = underlyingBuffer.duplicate().order(ByteOrder.nativeOrder())
    nullCount = ByteBufferHelper.getInt(nullsBuffer)
    nextNullIndex = if (nullCount > 0) ByteBufferHelper.getInt(nullsBuffer) else -1
    pos = 0

    underlyingBuffer.position(underlyingBuffer.position + 4 + nullCount * 4)
    super.initialize()
  }

  abstract override def extractTo(row: InternalRow, ordinal: Int): Unit = {
    if (pos == nextNullIndex) {
      seenNulls += 1

      if (seenNulls < nullCount) {
        nextNullIndex = ByteBufferHelper.getInt(nullsBuffer)
      }

      row.setNullAt(ordinal)
    } else {
      super.extractTo(row, ordinal)
    }

    pos += 1
  }

  abstract override def hasNext: Boolean = seenNulls < nullCount || super.hasNext
}
