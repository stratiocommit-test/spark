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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * A test suite for testing [[ExpressionEvalHelper]].
 *
 * Yes, we should write test cases for test harnesses, in case
 * they have behaviors that are easy to break.
 */
class ExpressionEvalHelperSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("SPARK-16489 checkEvaluation should fail if expression reuses variable names") {
    val e = intercept[RuntimeException] { checkEvaluation(BadCodegenExpression(), 10) }
    assert(e.getMessage.contains("some_variable"))
  }
}

/**
 * An expression that generates bad code (variable name "some_variable" is not unique across
 * instances of the expression.
 */
case class BadCodegenExpression() extends LeafExpression {
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any = 10
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.copy(code =
      s"""
        |int some_variable = 11;
        |int ${ev.value} = 10;
      """.stripMargin)
  }
  override def dataType: DataType = IntegerType
}
