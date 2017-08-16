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

import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._
import org.apache.spark.sql.types.AtomicType

class DictionaryEncodingSuite extends SparkFunSuite {
  testDictionaryEncoding(new IntColumnStats, INT)
  testDictionaryEncoding(new LongColumnStats, LONG)
  testDictionaryEncoding(new StringColumnStats, STRING)

  def testDictionaryEncoding[T <: AtomicType](
      columnStats: ColumnStats,
      columnType: NativeColumnType[T]) {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    def buildDictionary(buffer: ByteBuffer) = {
      (0 until buffer.getInt()).map(columnType.extract(buffer) -> _.toShort).toMap
    }

    def stableDistinct(seq: Seq[Int]): Seq[Int] = if (seq.isEmpty) {
      Seq.empty
    } else {
      seq.head +: seq.tail.filterNot(_ == seq.head)
    }

    def skeleton(uniqueValueCount: Int, inputSeq: Seq[Int]) {
      // -------------
      // Tests encoder
      // -------------

      val builder = TestCompressibleColumnBuilder(columnStats, columnType, DictionaryEncoding)
      val (values, rows) = makeUniqueValuesAndSingleValueRows(columnType, uniqueValueCount)
      val dictValues = stableDistinct(inputSeq)

      inputSeq.foreach(i => builder.appendFrom(rows(i), 0))

      if (dictValues.length > DictionaryEncoding.MAX_DICT_SIZE) {
        withClue("Dictionary overflowed, compression should fail") {
          intercept[Throwable] {
            builder.build()
          }
        }
      } else {
        val buffer = builder.build()
        val headerSize = CompressionScheme.columnHeaderSize(buffer)
        // 4 extra bytes for dictionary size
        val dictionarySize = 4 + rows.map(columnType.actualSize(_, 0)).sum
        // 2 bytes for each `Short`
        val compressedSize = 4 + dictionarySize + 2 * inputSeq.length
        // 4 extra bytes for compression scheme type ID
        assertResult(headerSize + compressedSize, "Wrong buffer capacity")(buffer.capacity)

        // Skips column header
        buffer.position(headerSize)
        assertResult(DictionaryEncoding.typeId, "Wrong compression scheme ID")(buffer.getInt())

        val dictionary = buildDictionary(buffer).toMap

        dictValues.foreach { i =>
          assertResult(i, "Wrong dictionary entry") {
            dictionary(values(i))
          }
        }

        inputSeq.foreach { i =>
          assertResult(i.toShort, "Wrong column element value")(buffer.getShort())
        }

        // -------------
        // Tests decoder
        // -------------

        // Rewinds, skips column header and 4 more bytes for compression scheme ID
        buffer.rewind().position(headerSize + 4)

        val decoder = DictionaryEncoding.decoder(buffer, columnType)
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
    }

    test(s"$DictionaryEncoding with $typeName: empty") {
      skeleton(0, Seq.empty)
    }

    test(s"$DictionaryEncoding with $typeName: simple case") {
      skeleton(2, Seq(0, 1, 0, 1))
    }

    test(s"$DictionaryEncoding with $typeName: dictionary overflow") {
      skeleton(DictionaryEncoding.MAX_DICT_SIZE + 1, 0 to DictionaryEncoding.MAX_DICT_SIZE)
    }
  }
}
