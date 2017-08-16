/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Compute Pearson correlation between two expressions.
 * When applied on empty data (i.e., count is zero), it returns NULL.
 *
 * Definition of Pearson correlation can be found at
 * http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns Pearson coefficient of correlation between a set of number pairs.")
// scalastyle:on line.size.limit
case class Corr(x: Expression, y: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = Seq(x, y)
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  protected val n = AttributeReference("n", DoubleType, nullable = false)()
  protected val xAvg = AttributeReference("xAvg", DoubleType, nullable = false)()
  protected val yAvg = AttributeReference("yAvg", DoubleType, nullable = false)()
  protected val ck = AttributeReference("ck", DoubleType, nullable = false)()
  protected val xMk = AttributeReference("xMk", DoubleType, nullable = false)()
  protected val yMk = AttributeReference("yMk", DoubleType, nullable = false)()

  override val aggBufferAttributes: Seq[AttributeReference] = Seq(n, xAvg, yAvg, ck, xMk, yMk)

  override val initialValues: Seq[Expression] = Array.fill(6)(Literal(0.0))

  override val updateExpressions: Seq[Expression] = {
    val newN = n + Literal(1.0)
    val dx = x - xAvg
    val dxN = dx / newN
    val dy = y - yAvg
    val dyN = dy / newN
    val newXAvg = xAvg + dxN
    val newYAvg = yAvg + dyN
    val newCk = ck + dx * (y - newYAvg)
    val newXMk = xMk + dx * (x - newXAvg)
    val newYMk = yMk + dy * (y - newYAvg)

    val isNull = IsNull(x) || IsNull(y)
    Seq(
      If(isNull, n, newN),
      If(isNull, xAvg, newXAvg),
      If(isNull, yAvg, newYAvg),
      If(isNull, ck, newCk),
      If(isNull, xMk, newXMk),
      If(isNull, yMk, newYMk)
    )
  }

  override val mergeExpressions: Seq[Expression] = {

    val n1 = n.left
    val n2 = n.right
    val newN = n1 + n2
    val dx = xAvg.right - xAvg.left
    val dxN = If(newN === Literal(0.0), Literal(0.0), dx / newN)
    val dy = yAvg.right - yAvg.left
    val dyN = If(newN === Literal(0.0), Literal(0.0), dy / newN)
    val newXAvg = xAvg.left + dxN * n2
    val newYAvg = yAvg.left + dyN * n2
    val newCk = ck.left + ck.right + dx * dyN * n1 * n2
    val newXMk = xMk.left + xMk.right + dx * dxN * n1 * n2
    val newYMk = yMk.left + yMk.right + dy * dyN * n1 * n2

    Seq(newN, newXAvg, newYAvg, newCk, newXMk, newYMk)
  }

  override val evaluateExpression: Expression = {
    If(n === Literal(0.0), Literal.create(null, DoubleType),
      If(n === Literal(1.0), Literal(Double.NaN),
        ck / Sqrt(xMk * yMk)))
  }

  override def prettyName: String = "corr"
}
