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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.columnar.{ColumnBuilder, NativeColumnBuilder}
import org.apache.spark.sql.types.AtomicType
import org.apache.spark.unsafe.Platform

/**
 * A stackable trait that builds optionally compressed byte buffer for a column.  Memory layout of
 * the final byte buffer is:
 * {{{
 *    .----------------------- Null count N (4 bytes)
 *    |   .------------------- Null positions (4 x N bytes, empty if null count is zero)
 *    |   |     .------------- Compression scheme ID (4 bytes)
 *    |   |     |   .--------- Compressed non-null elements
 *    V   V     V   V
 *   +---+-----+---+---------+
 *   |   | ... |   | ... ... |
 *   +---+-----+---+---------+
 *    \-------/ \-----------/
 *     header         body
 * }}}
 */
private[columnar] trait CompressibleColumnBuilder[T <: AtomicType]
  extends ColumnBuilder with Logging {

  this: NativeColumnBuilder[T] with WithCompressionSchemes =>

  var compressionEncoders: Seq[Encoder[T]] = _

  abstract override def initialize(
      initialSize: Int,
      columnName: String,
      useCompression: Boolean): Unit = {

    compressionEncoders =
      if (useCompression) {
        schemes.filter(_.supports(columnType)).map(_.encoder[T](columnType))
      } else {
        Seq(PassThrough.encoder(columnType))
      }
    super.initialize(initialSize, columnName, useCompression)
  }

  // The various compression schemes, while saving memory use, cause all of the data within
  // the row to become unaligned, thus causing crashes.  Until a way of fixing the compression
  // is found to also allow aligned accesses this must be disabled for SPARC.

  protected def isWorthCompressing(encoder: Encoder[T]) = {
    CompressibleColumnBuilder.unaligned && encoder.compressionRatio < 0.8
  }

  private def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
    compressionEncoders.foreach(_.gatherCompressibilityStats(row, ordinal))
  }

  abstract override def appendFrom(row: InternalRow, ordinal: Int): Unit = {
    super.appendFrom(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      gatherCompressibilityStats(row, ordinal)
    }
  }

  override def build(): ByteBuffer = {
    val nonNullBuffer = buildNonNulls()
    val encoder: Encoder[T] = {
      val candidate = compressionEncoders.minBy(_.compressionRatio)
      if (isWorthCompressing(candidate)) candidate else PassThrough.encoder(columnType)
    }

    // Header = null count + null positions
    val headerSize = 4 + nulls.limit()
    val compressedSize = if (encoder.compressedSize == 0) {
      nonNullBuffer.remaining()
    } else {
      encoder.compressedSize
    }

    val compressedBuffer = ByteBuffer
      // Reserves 4 bytes for compression scheme ID
      .allocate(headerSize + 4 + compressedSize)
      .order(ByteOrder.nativeOrder)
      // Write the header
      .putInt(nullCount)
      .put(nulls)

    logDebug(s"Compressor for [$columnName]: $encoder, ratio: ${encoder.compressionRatio}")
    encoder.compress(nonNullBuffer, compressedBuffer)
  }
}

private[columnar] object CompressibleColumnBuilder {
  val unaligned = Platform.unaligned()
}
