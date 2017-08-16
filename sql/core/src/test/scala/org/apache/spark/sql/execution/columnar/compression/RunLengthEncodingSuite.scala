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
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
import org.apache.spark.sql.types.AtomicType

class RunLengthEncodingSuite extends SparkFunSuite {
  testRunLengthEncoding(new NoopColumnStats, BOOLEAN)
  testRunLengthEncoding(new ByteColumnStats, BYTE)
  testRunLengthEncoding(new ShortColumnStats, SHORT)
  testRunLengthEncoding(new IntColumnStats, INT)
  testRunLengthEncoding(new LongColumnStats, LONG)
  testRunLengthEncoding(new StringColumnStats, STRING)

  def testRunLengthEncoding[T <: AtomicType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T]) {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    def skeleton(uniqueValueCount: Int, inputRuns: Seq[(Int, Int)]) {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, RunLengthEncoding)
      val (values, rows) = makeUniqueValuesAndSingleValueRows(columnType, uniqueValueCount)
      val inputSeq = inputRuns.flatMap { case (index, run) =>
        Seq.fill(run)(index)
      }

      inputSeq.foreach(i => builder.appendFrom(rows(i), 0))
      val buffer = builder.build()

      // Column type ID + null count + null positions
      val headerSize = CompressionScheme.columnHeaderSize(buffer)

      // Compression scheme ID + compressed contents
      val compressedSize = 4 + inputRuns.map { case (index, _) =>
        // 4 extra bytes each run for run length
        columnType.actualSize(rows(index), 0) + 4
      }.sum

      // 4 extra bytes for compression scheme type ID
      assertResult(headerSize + compressedSize, "Wrong buffer capacity")(buffer.capacity)

      // Skips column header
      buffer.position(headerSize)
      assertResult(RunLengthEncoding.typeId, "Wrong compression scheme ID")(buffer.getInt())

      inputRuns.foreach { case (index, run) =>
        assertResult(values(index), "Wrong column element value")(columnType.extract(buffer))
        assertResult(run, "Wrong run length")(buffer.getInt())
      }

      // -------------
      // Tests decoder
      // -------------

      // Rewinds, skips column header and 4 more bytes for compression scheme ID
      buffer.rewind().position(headerSize + 4)

      val decoder = RunLengthEncoding.decoder(buffer, columnType)
      val mutableRow = new GenericInternalRow(1)

      if (inputSeq.nonEmpty) {
        inputSeq.foreach { i =>
          assert(decoder.hasNext)
          assertResult(values(i), "Wrong decoded value") {
            decoder.next(mutableRow, 0)
            columnType.getField(mutableRow, 0)
          }
        }
      }

      assert(!decoder.hasNext)
    }

    test(s"$RunLengthEncoding with $typeName: empty column") {
      skeleton(0, Seq.empty)
    }

    test(s"$RunLengthEncoding with $typeName: simple case") {
      skeleton(2, Seq(0 -> 2, 1 -> 2))
    }

    test(s"$RunLengthEncoding with $typeName: run length == 1") {
      skeleton(2, Seq(0 -> 1, 1 -> 1))
    }

    test(s"$RunLengthEncoding with $typeName: single long run") {
      skeleton(1, Seq(0 -> 1000))
    }
  }
}
