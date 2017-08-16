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
import org.apache.spark.sql.types._

/**
 * Return the unscaled Long value of a Decimal, assuming it fits in a Long.
 * Note: this expression is internal and created only by the optimizer,
 * we don't need to do type check for it.
 */
case class UnscaledValue(child: Expression) extends UnaryExpression {

  override def dataType: DataType = LongType
  override def toString: String = s"UnscaledValue($child)"

  protected override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[Decimal].toUnscaledLong

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"$c.toUnscaledLong()")
  }
}

/**
 * Create a Decimal from an unscaled Long value.
 * Note: this expression is internal and created only by the optimizer,
 * we don't need to do type check for it.
 */
case class MakeDecimal(child: Expression, precision: Int, scale: Int) extends UnaryExpression {

  override def dataType: DataType = DecimalType(precision, scale)
  override def nullable: Boolean = true
  override def toString: String = s"MakeDecimal($child,$precision,$scale)"

  protected override def nullSafeEval(input: Any): Any =
    Decimal(input.asInstanceOf[Long], precision, scale)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        ${ev.value} = (new Decimal()).setOrNull($eval, $precision, $scale);
        ${ev.isNull} = ${ev.value} == null;
      """
    })
  }
}

/**
 * An expression used to wrap the children when promote the precision of DecimalType to avoid
 * promote multiple times.
 */
case class PromotePrecision(child: Expression) extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override def eval(input: InternalRow): Any = child.eval(input)
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev.copy("")
  override def prettyName: String = "promote_precision"
  override def sql: String = child.sql
}

/**
 * Rounds the decimal to given scale and check whether the decimal can fit in provided precision
 * or not, returns null if not.
 */
case class CheckOverflow(child: Expression, dataType: DecimalType) extends UnaryExpression {

  override def nullable: Boolean = true

  override def nullSafeEval(input: Any): Any = {
    val d = input.asInstanceOf[Decimal].clone()
    if (d.changePrecision(dataType.precision, dataType.scale)) {
      d
    } else {
      null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      val tmp = ctx.freshName("tmp")
      s"""
         | Decimal $tmp = $eval.clone();
         | if ($tmp.changePrecision(${dataType.precision}, ${dataType.scale})) {
         |   ${ev.value} = $tmp;
         | } else {
         |   ${ev.isNull} = true;
         | }
       """.stripMargin
    })
  }

  override def toString: String = s"CheckOverflow($child, $dataType)"

  override def sql: String = child.sql
}
