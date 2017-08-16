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

import scala.collection.immutable.HashSet
import scala.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.{AtomicType, Decimal}
import org.apache.spark.unsafe.types.UTF8String

object ColumnarTestUtils {
  def makeNullRow(length: Int): GenericInternalRow = {
    val row = new GenericInternalRow(length)
    (0 until length).foreach(row.setNullAt)
    row
  }

  def makeRandomValue[JvmType](columnType: ColumnType[JvmType]): JvmType = {
    def randomBytes(length: Int) = {
      val bytes = new Array[Byte](length)
      Random.nextBytes(bytes)
      bytes
    }

    (columnType match {
      case NULL => null
      case BOOLEAN => Random.nextBoolean()
      case BYTE => (Random.nextInt(Byte.MaxValue * 2) - Byte.MaxValue).toByte
      case SHORT => (Random.nextInt(Short.MaxValue * 2) - Short.MaxValue).toShort
      case INT => Random.nextInt()
      case LONG => Random.nextLong()
      case FLOAT => Random.nextFloat()
      case DOUBLE => Random.nextDouble()
      case STRING => UTF8String.fromString(Random.nextString(Random.nextInt(32)))
      case BINARY => randomBytes(Random.nextInt(32))
      case COMPACT_DECIMAL(precision, scale) => Decimal(Random.nextLong() % 100, precision, scale)
      case LARGE_DECIMAL(precision, scale) => Decimal(Random.nextLong(), precision, scale)
      case STRUCT(_) =>
        new GenericInternalRow(Array[Any](UTF8String.fromString(Random.nextString(10))))
      case ARRAY(_) =>
        new GenericArrayData(Array[Any](Random.nextInt(), Random.nextInt()))
      case MAP(_) =>
        ArrayBasedMapData(
          Map(Random.nextInt() -> UTF8String.fromString(Random.nextString(Random.nextInt(32)))))
      case _ => throw new IllegalArgumentException(s"Unknown column type $columnType")
    }).asInstanceOf[JvmType]
  }

  def makeRandomValues(
      head: ColumnType[_],
      tail: ColumnType[_]*): Seq[Any] = makeRandomValues(Seq(head) ++ tail)

  def makeRandomValues(columnTypes: Seq[ColumnType[_]]): Seq[Any] = {
    columnTypes.map(makeRandomValue(_))
  }

  def makeUniqueRandomValues[JvmType](
      columnType: ColumnType[JvmType],
      count: Int): Seq[JvmType] = {

    Iterator.iterate(HashSet.empty[JvmType]) { set =>
      set + Iterator.continually(makeRandomValue(columnType)).filterNot(set.contains).next()
    }.drop(count).next().toSeq
  }

  def makeRandomRow(
      head: ColumnType[_],
      tail: ColumnType[_]*): InternalRow = makeRandomRow(Seq(head) ++ tail)

  def makeRandomRow(columnTypes: Seq[ColumnType[_]]): InternalRow = {
    val row = new GenericInternalRow(columnTypes.length)
    makeRandomValues(columnTypes).zipWithIndex.foreach { case (value, index) =>
      row(index) = value
    }
    row
  }

  def makeUniqueValuesAndSingleValueRows[T <: AtomicType](
      columnType: NativeColumnType[T],
      count: Int): (Seq[T#InternalType], Seq[GenericInternalRow]) = {

    val values = makeUniqueRandomValues(columnType, count)
    val rows = values.map { value =>
      val row = new GenericInternalRow(1)
      row(0) = value
      row
    }

    (values, rows)
  }
}
