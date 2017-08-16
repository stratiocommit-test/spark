/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, LongType}

/**
 * Returns monotonically increasing 64-bit integers.
 *
 * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
 * The current implementation puts the partition ID in the upper 31 bits, and the lower 33 bits
 * represent the record number within each partition. The assumption is that the data frame has
 * less than 1 billion partitions, and each partition has less than 8 billion records.
 *
 * Since this expression is stateful, it cannot be a case object.
 */
@ExpressionDescription(
  usage = """
    _FUNC_() - Returns monotonically increasing 64-bit integers. The generated ID is guaranteed
      to be monotonically increasing and unique, but not consecutive. The current implementation
      puts the partition ID in the upper 31 bits, and the lower 33 bits represent the record number
      within each partition. The assumption is that the data frame has less than 1 billion
      partitions, and each partition has less than 8 billion records.
  """)
case class MonotonicallyIncreasingID() extends LeafExpression with Nondeterministic {

  /**
   * Record ID within each partition. By being transient, count's value is reset to 0 every time
   * we serialize and deserialize and initialize it.
   */
  @transient private[this] var count: Long = _

  @transient private[this] var partitionMask: Long = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    count = 0L
    partitionMask = partitionIndex.toLong << 33
  }

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override protected def evalInternal(input: InternalRow): Long = {
    val currentCount = count
    count += 1
    partitionMask + currentCount
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val countTerm = ctx.freshName("count")
    val partitionMaskTerm = ctx.freshName("partitionMask")
    ctx.addMutableState(ctx.JAVA_LONG, countTerm, "")
    ctx.addMutableState(ctx.JAVA_LONG, partitionMaskTerm, "")
    ctx.addPartitionInitializationStatement(s"$countTerm = 0L;")
    ctx.addPartitionInitializationStatement(s"$partitionMaskTerm = ((long) partitionIndex) << 33;")

    ev.copy(code = s"""
      final ${ctx.javaType(dataType)} ${ev.value} = $partitionMaskTerm + $countTerm;
      $countTerm++;""", isNull = "false")
  }

  override def prettyName: String = "monotonically_increasing_id"

  override def sql: String = s"$prettyName()"
}
