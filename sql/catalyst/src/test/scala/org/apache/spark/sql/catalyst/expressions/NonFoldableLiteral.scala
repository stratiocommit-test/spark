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
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._


/**
 * A literal value that is not foldable. Used in expression codegen testing to test code path
 * that behave differently based on foldable values.
 */
case class NonFoldableLiteral(value: Any, dataType: DataType) extends LeafExpression {

  override def foldable: Boolean = false
  override def nullable: Boolean = true

  override def toString: String = if (value != null) value.toString else "null"

  override def eval(input: InternalRow): Any = value

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    Literal.create(value, dataType).doGenCode(ctx, ev)
  }
}


object NonFoldableLiteral {
  def apply(value: Any): NonFoldableLiteral = {
    val lit = Literal(value)
    NonFoldableLiteral(lit.value, lit.dataType)
  }
  def create(value: Any, dataType: DataType): NonFoldableLiteral = {
    val lit = Literal.create(value, dataType)
    NonFoldableLiteral(lit.value, lit.dataType)
  }
}
