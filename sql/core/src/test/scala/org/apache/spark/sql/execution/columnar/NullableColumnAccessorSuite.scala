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

import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.types._

class TestNullableColumnAccessor[JvmType](
    buffer: ByteBuffer,
    columnType: ColumnType[JvmType])
  extends BasicColumnAccessor(buffer, columnType)
  with NullableColumnAccessor

object TestNullableColumnAccessor {
  def apply[JvmType](buffer: ByteBuffer, columnType: ColumnType[JvmType])
    : TestNullableColumnAccessor[JvmType] = {
    new TestNullableColumnAccessor(buffer, columnType)
  }
}

class NullableColumnAccessorSuite extends SparkFunSuite {
  import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

  Seq(
    NULL, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE,
    STRING, BINARY, COMPACT_DECIMAL(15, 10), LARGE_DECIMAL(20, 10),
    STRUCT(StructType(StructField("a", StringType) :: Nil)),
    ARRAY(ArrayType(IntegerType)), MAP(MapType(IntegerType, StringType)))
    .foreach {
    testNullableColumnAccessor(_)
  }

  def testNullableColumnAccessor[JvmType](
      columnType: ColumnType[JvmType]): Unit = {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")
    val nullRow = makeNullRow(1)

    test(s"Nullable $typeName column accessor: empty column") {
      val builder = TestNullableColumnBuilder(columnType)
      val accessor = TestNullableColumnAccessor(builder.build(), columnType)
      assert(!accessor.hasNext)
    }

    test(s"Nullable $typeName column accessor: access null values") {
      val builder = TestNullableColumnBuilder(columnType)
      val randomRow = makeRandomRow(columnType)
      val proj = UnsafeProjection.create(Array[DataType](columnType.dataType))

      (0 until 4).foreach { _ =>
        builder.appendFrom(proj(randomRow), 0)
        builder.appendFrom(proj(nullRow), 0)
      }

      val accessor = TestNullableColumnAccessor(builder.build(), columnType)
      val row = new GenericInternalRow(1)
      val converter = CatalystTypeConverters.createToScalaConverter(columnType.dataType)

      (0 until 4).foreach { _ =>
        assert(accessor.hasNext)
        accessor.extractTo(row, 0)
        assert(converter(row.get(0, columnType.dataType))
          === converter(randomRow.get(0, columnType.dataType)))

        assert(accessor.hasNext)
        accessor.extractTo(row, 0)
        assert(row.isNullAt(0))
      }

      assert(!accessor.hasNext)
    }
  }
}
