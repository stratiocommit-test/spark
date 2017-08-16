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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * Expression that returns the current partition id.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current partition id.")
case class SparkPartitionID() extends LeafExpression with Nondeterministic {

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  @transient private[this] var partitionId: Int = _

  override val prettyName = "SPARK_PARTITION_ID"

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    partitionId = partitionIndex
  }

  override protected def evalInternal(input: InternalRow): Int = partitionId

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val idTerm = ctx.freshName("partitionId")
    ctx.addMutableState(ctx.JAVA_INT, idTerm, "")
    ctx.addPartitionInitializationStatement(s"$idTerm = partitionIndex;")
    ev.copy(code = s"final ${ctx.javaType(dataType)} ${ev.value} = $idTerm;", isNull = "false")
  }
}
