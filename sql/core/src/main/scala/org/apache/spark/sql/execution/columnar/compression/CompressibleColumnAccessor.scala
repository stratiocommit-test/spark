/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.columnar.compression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.columnar.{ColumnAccessor, NativeColumnAccessor}
import org.apache.spark.sql.types.AtomicType

private[columnar] trait CompressibleColumnAccessor[T <: AtomicType] extends ColumnAccessor {
  this: NativeColumnAccessor[T] =>

  private var decoder: Decoder[T] = _

  abstract override protected def initialize(): Unit = {
    super.initialize()
    decoder = CompressionScheme(underlyingBuffer.getInt()).decoder(buffer, columnType)
  }

  abstract override def hasNext: Boolean = super.hasNext || decoder.hasNext

  override def extractSingle(row: InternalRow, ordinal: Int): Unit = {
    decoder.next(row, ordinal)
  }
}
