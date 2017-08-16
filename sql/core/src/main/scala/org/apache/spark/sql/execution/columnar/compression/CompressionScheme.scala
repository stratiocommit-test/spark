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

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.columnar.{ColumnType, NativeColumnType}
import org.apache.spark.sql.types.AtomicType

private[columnar] trait Encoder[T <: AtomicType] {
  def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {}

  def compressedSize: Int

  def uncompressedSize: Int

  def compressionRatio: Double = {
    if (uncompressedSize > 0) compressedSize.toDouble / uncompressedSize else 1.0
  }

  def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer
}

private[columnar] trait Decoder[T <: AtomicType] {
  def next(row: InternalRow, ordinal: Int): Unit

  def hasNext: Boolean
}

private[columnar] trait CompressionScheme {
  def typeId: Int

  def supports(columnType: ColumnType[_]): Boolean

  def encoder[T <: AtomicType](columnType: NativeColumnType[T]): Encoder[T]

  def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T]): Decoder[T]
}

private[columnar] trait WithCompressionSchemes {
  def schemes: Seq[CompressionScheme]
}

private[columnar] trait AllCompressionSchemes extends WithCompressionSchemes {
  override val schemes: Seq[CompressionScheme] = CompressionScheme.all
}

private[columnar] object CompressionScheme {
  val all: Seq[CompressionScheme] =
    Seq(PassThrough, RunLengthEncoding, DictionaryEncoding, BooleanBitSet, IntDelta, LongDelta)

  private val typeIdToScheme = all.map(scheme => scheme.typeId -> scheme).toMap

  def apply(typeId: Int): CompressionScheme = {
    typeIdToScheme.getOrElse(typeId, throw new UnsupportedOperationException(
      s"Unrecognized compression scheme type ID: $typeId"))
  }

  def columnHeaderSize(columnBuffer: ByteBuffer): Int = {
    val header = columnBuffer.duplicate().order(ByteOrder.nativeOrder)
    val nullCount = header.getInt()
    // null count + null positions
    4 + 4 * nullCount
  }
}
