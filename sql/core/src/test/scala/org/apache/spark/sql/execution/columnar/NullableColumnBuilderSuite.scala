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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.types._

class TestNullableColumnBuilder[JvmType](columnType: ColumnType[JvmType])
  extends BasicColumnBuilder[JvmType](new NoopColumnStats, columnType)
  with NullableColumnBuilder

object TestNullableColumnBuilder {
  def apply[JvmType](columnType: ColumnType[JvmType], initialSize: Int = 0)
    : TestNullableColumnBuilder[JvmType] = {
    val builder = new TestNullableColumnBuilder(columnType)
    builder.initialize(initialSize)
    builder
  }
}

class NullableColumnBuilderSuite extends SparkFunSuite {
  import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

  Seq(
    BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE,
    STRING, BINARY, COMPACT_DECIMAL(15, 10), LARGE_DECIMAL(20, 10),
    STRUCT(StructType(StructField("a", StringType) :: Nil)),
    ARRAY(ArrayType(IntegerType)), MAP(MapType(IntegerType, StringType)))
    .foreach {
    testNullableColumnBuilder(_)
  }

  def testNullableColumnBuilder[JvmType](
      columnType: ColumnType[JvmType]): Unit = {

    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")
    val dataType = columnType.dataType
    val proj = UnsafeProjection.create(Array[DataType](dataType))
    val converter = CatalystTypeConverters.createToScalaConverter(dataType)

    test(s"$typeName column builder: empty column") {
      val columnBuilder = TestNullableColumnBuilder(columnType)
      val buffer = columnBuilder.build()

      assertResult(0, "Wrong null count")(buffer.getInt())
      assert(!buffer.hasRemaining)
    }

    test(s"$typeName column builder: buffer size auto growth") {
      val columnBuilder = TestNullableColumnBuilder(columnType)
      val randomRow = makeRandomRow(columnType)

      (0 until 4).foreach { _ =>
        columnBuilder.appendFrom(proj(randomRow), 0)
      }

      val buffer = columnBuilder.build()

      assertResult(0, "Wrong null count")(buffer.getInt())
    }

    test(s"$typeName column builder: null values") {
      val columnBuilder = TestNullableColumnBuilder(columnType)
      val randomRow = makeRandomRow(columnType)
      val nullRow = makeNullRow(1)

      (0 until 4).foreach { _ =>
        columnBuilder.appendFrom(proj(randomRow), 0)
        columnBuilder.appendFrom(proj(nullRow), 0)
      }

      val buffer = columnBuilder.build()

      assertResult(4, "Wrong null count")(buffer.getInt())

      // For null positions
      (1 to 7 by 2).foreach(assertResult(_, "Wrong null position")(buffer.getInt()))

      // For non-null values
      val actual = new GenericInternalRow(new Array[Any](1))
      (0 until 4).foreach { _ =>
        columnType.extract(buffer, actual, 0)
        assert(converter(actual.get(0, dataType)) === converter(randomRow.get(0, dataType)),
          "Extracted value didn't equal to the original one")
      }

      assert(!buffer.hasRemaining)
    }
  }
}
