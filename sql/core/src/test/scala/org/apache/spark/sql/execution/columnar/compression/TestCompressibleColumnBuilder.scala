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

import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.types.AtomicType

class TestCompressibleColumnBuilder[T <: AtomicType](
    override val columnStats: ColumnStats,
    override val columnType: NativeColumnType[T],
    override val schemes: Seq[CompressionScheme])
  extends NativeColumnBuilder(columnStats, columnType)
  with NullableColumnBuilder
  with CompressibleColumnBuilder[T] {

  override protected def isWorthCompressing(encoder: Encoder[T]) = true
}

object TestCompressibleColumnBuilder {
  def apply[T <: AtomicType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T],
      scheme: CompressionScheme): TestCompressibleColumnBuilder[T] = {

    val builder = new TestCompressibleColumnBuilder(columnStats, columnType, Seq(scheme))
    builder.initialize(0, "", useCompression = true)
    builder
  }
}
