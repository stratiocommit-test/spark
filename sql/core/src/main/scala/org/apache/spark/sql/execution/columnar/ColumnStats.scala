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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, GenericInternalRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[columnar] class ColumnStatisticsSchema(a: Attribute) extends Serializable {
  val upperBound = AttributeReference(a.name + ".upperBound", a.dataType, nullable = true)()
  val lowerBound = AttributeReference(a.name + ".lowerBound", a.dataType, nullable = true)()
  val nullCount = AttributeReference(a.name + ".nullCount", IntegerType, nullable = false)()
  val count = AttributeReference(a.name + ".count", IntegerType, nullable = false)()
  val sizeInBytes = AttributeReference(a.name + ".sizeInBytes", LongType, nullable = false)()

  val schema = Seq(lowerBound, upperBound, nullCount, count, sizeInBytes)
}

private[columnar] class PartitionStatistics(tableSchema: Seq[Attribute]) extends Serializable {
  val (forAttribute: AttributeMap[ColumnStatisticsSchema], schema: Seq[AttributeReference]) = {
    val allStats = tableSchema.map(a => a -> new ColumnStatisticsSchema(a))
    (AttributeMap(allStats), allStats.flatMap(_._2.schema))
  }
}

/**
 * Used to collect statistical information when building in-memory columns.
 *
 * NOTE: we intentionally avoid using `Ordering[T]` to compare values here because `Ordering[T]`
 * brings significant performance penalty.
 */
private[columnar] sealed trait ColumnStats extends Serializable {
  protected var count = 0
  protected var nullCount = 0
  private[columnar] var sizeInBytes = 0L

  /**
   * Gathers statistics information from `row(ordinal)`.
   */
  def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (row.isNullAt(ordinal)) {
      nullCount += 1
      // 4 bytes for null position
      sizeInBytes += 4
    }
    count += 1
  }

  /**
   * Column statistics represented as a single row, currently including closed lower bound, closed
   * upper bound and null count.
   */
  def collectedStatistics: GenericInternalRow
}

/**
 * A no-op ColumnStats only used for testing purposes.
 */
private[columnar] class NoopColumnStats extends ColumnStats {
  override def gatherStats(row: InternalRow, ordinal: Int): Unit = super.gatherStats(row, ordinal)

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](null, null, nullCount, count, 0L))
}

private[columnar] class BooleanColumnStats extends ColumnStats {
  protected var upper = false
  protected var lower = true

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getBoolean(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += BOOLEAN.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[columnar] class ByteColumnStats extends ColumnStats {
  protected var upper = Byte.MinValue
  protected var lower = Byte.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getByte(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += BYTE.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[columnar] class ShortColumnStats extends ColumnStats {
  protected var upper = Short.MinValue
  protected var lower = Short.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getShort(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += SHORT.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[columnar] class IntColumnStats extends ColumnStats {
  protected var upper = Int.MinValue
  protected var lower = Int.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getInt(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += INT.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[columnar] class LongColumnStats extends ColumnStats {
  protected var upper = Long.MinValue
  protected var lower = Long.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getLong(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += LONG.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[columnar] class FloatColumnStats extends ColumnStats {
  protected var upper = Float.MinValue
  protected var lower = Float.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getFloat(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += FLOAT.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[columnar] class DoubleColumnStats extends ColumnStats {
  protected var upper = Double.MinValue
  protected var lower = Double.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getDouble(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += DOUBLE.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[columnar] class StringColumnStats extends ColumnStats {
  protected var upper: UTF8String = null
  protected var lower: UTF8String = null

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getUTF8String(ordinal)
      if (upper == null || value.compareTo(upper) > 0) upper = value.clone()
      if (lower == null || value.compareTo(lower) < 0) lower = value.clone()
      sizeInBytes += STRING.actualSize(row, ordinal)
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[columnar] class BinaryColumnStats extends ColumnStats {
  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      sizeInBytes += BINARY.actualSize(row, ordinal)
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](null, null, nullCount, count, sizeInBytes))
}

private[columnar] class DecimalColumnStats(precision: Int, scale: Int) extends ColumnStats {
  def this(dt: DecimalType) = this(dt.precision, dt.scale)

  protected var upper: Decimal = null
  protected var lower: Decimal = null

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getDecimal(ordinal, precision, scale)
      if (upper == null || value.compareTo(upper) > 0) upper = value
      if (lower == null || value.compareTo(lower) < 0) lower = value
      // TODO: this is not right for DecimalType with precision > 18
      sizeInBytes += 8
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[columnar] class ObjectColumnStats(dataType: DataType) extends ColumnStats {
  val columnType = ColumnType(dataType)

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      sizeInBytes += columnType.actualSize(row, ordinal)
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](null, null, nullCount, count, sizeInBytes))
}
