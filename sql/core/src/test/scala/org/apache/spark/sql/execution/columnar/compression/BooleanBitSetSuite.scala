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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.columnar.{BOOLEAN, NoopColumnStats}
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

class BooleanBitSetSuite extends SparkFunSuite {
  import BooleanBitSet._

  def skeleton(count: Int) {
    // -------------
    // Tests encoder
    // -------------

    val builder = TestCompressibleColumnBuilder(new NoopColumnStats, BOOLEAN, BooleanBitSet)
    val rows = Seq.fill[InternalRow](count)(makeRandomRow(BOOLEAN))
    val values = rows.map(_.getBoolean(0))

    rows.foreach(builder.appendFrom(_, 0))
    val buffer = builder.build()

    // Column type ID + null count + null positions
    val headerSize = CompressionScheme.columnHeaderSize(buffer)

    // Compression scheme ID + element count + bitset words
    val compressedSize = 4 + 4 + {
      val extra = if (count % BITS_PER_LONG == 0) 0 else 1
      (count / BITS_PER_LONG + extra) * 8
    }

    // 4 extra bytes for compression scheme type ID
    assertResult(headerSize + compressedSize, "Wrong buffer capacity")(buffer.capacity)

    // Skips column header
    buffer.position(headerSize)
    assertResult(BooleanBitSet.typeId, "Wrong compression scheme ID")(buffer.getInt())
    assertResult(count, "Wrong element count")(buffer.getInt())

    var word = 0: Long
    for (i <- 0 until count) {
      val bit = i % BITS_PER_LONG
      word = if (bit == 0) buffer.getLong() else word
      assertResult(values(i), s"Wrong value in compressed buffer, index=$i") {
        (word & ((1: Long) << bit)) != 0
      }
    }

    // -------------
    // Tests decoder
    // -------------

    // Rewinds, skips column header and 4 more bytes for compression scheme ID
    buffer.rewind().position(headerSize + 4)

    val decoder = BooleanBitSet.decoder(buffer, BOOLEAN)
    val mutableRow = new GenericInternalRow(1)
    if (values.nonEmpty) {
      values.foreach {
        assert(decoder.hasNext)
        assertResult(_, "Wrong decoded value") {
          decoder.next(mutableRow, 0)
          mutableRow.getBoolean(0)
        }
      }
    }
    assert(!decoder.hasNext)
  }

  test(s"$BooleanBitSet: empty") {
    skeleton(0)
  }

  test(s"$BooleanBitSet: less than 1 word") {
    skeleton(BITS_PER_LONG - 1)
  }

  test(s"$BooleanBitSet: exactly 1 word") {
    skeleton(BITS_PER_LONG)
  }

  test(s"$BooleanBitSet: multiple whole words") {
    skeleton(BITS_PER_LONG * 2)
  }

  test(s"$BooleanBitSet: multiple words and 1 more bit") {
    skeleton(BITS_PER_LONG * 2 + 1)
  }
}
