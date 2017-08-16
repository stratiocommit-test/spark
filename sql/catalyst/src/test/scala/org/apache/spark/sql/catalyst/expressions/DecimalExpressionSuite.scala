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
import org.apache.spark.sql.types.{Decimal, DecimalType, LongType}

class DecimalExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("UnscaledValue") {
    val d1 = Decimal("10.1")
    checkEvaluation(UnscaledValue(Literal(d1)), 101L)
    val d2 = Decimal(101, 3, 1)
    checkEvaluation(UnscaledValue(Literal(d2)), 101L)
    checkEvaluation(UnscaledValue(Literal.create(null, DecimalType(2, 1))), null)
  }

  test("MakeDecimal") {
    checkEvaluation(MakeDecimal(Literal(101L), 3, 1), Decimal("10.1"))
    checkEvaluation(MakeDecimal(Literal.create(null, LongType), 3, 1), null)
  }

  test("PromotePrecision") {
    val d1 = Decimal("10.1")
    checkEvaluation(PromotePrecision(Literal(d1)), d1)
    val d2 = Decimal(101, 3, 1)
    checkEvaluation(PromotePrecision(Literal(d2)), d2)
    checkEvaluation(PromotePrecision(Literal.create(null, DecimalType(2, 1))), null)
  }

  test("CheckOverflow") {
    val d1 = Decimal("10.1")
    checkEvaluation(CheckOverflow(Literal(d1), DecimalType(4, 0)), Decimal("10"))
    checkEvaluation(CheckOverflow(Literal(d1), DecimalType(4, 1)), d1)
    checkEvaluation(CheckOverflow(Literal(d1), DecimalType(4, 2)), d1)
    checkEvaluation(CheckOverflow(Literal(d1), DecimalType(4, 3)), null)

    val d2 = Decimal(101, 3, 1)
    checkEvaluation(CheckOverflow(Literal(d2), DecimalType(4, 0)), Decimal("10"))
    checkEvaluation(CheckOverflow(Literal(d2), DecimalType(4, 1)), d2)
    checkEvaluation(CheckOverflow(Literal(d2), DecimalType(4, 2)), d2)
    checkEvaluation(CheckOverflow(Literal(d2), DecimalType(4, 3)), null)

    checkEvaluation(CheckOverflow(Literal.create(null, DecimalType(2, 1)), DecimalType(3, 2)), null)
  }

}
