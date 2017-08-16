/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.trees

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Expression, IntegerLiteral, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}

class RuleExecutorSuite extends SparkFunSuite {
  object DecrementLiterals extends Rule[Expression] {
    def apply(e: Expression): Expression = e transform {
      case IntegerLiteral(i) if i > 0 => Literal(i - 1)
    }
  }

  test("only once") {
    object ApplyOnce extends RuleExecutor[Expression] {
      val batches = Batch("once", Once, DecrementLiterals) :: Nil
    }

    assert(ApplyOnce.execute(Literal(10)) === Literal(9))
  }

  test("to fixed point") {
    object ToFixedPoint extends RuleExecutor[Expression] {
      val batches = Batch("fixedPoint", FixedPoint(100), DecrementLiterals) :: Nil
    }

    assert(ToFixedPoint.execute(Literal(10)) === Literal(0))
  }

  test("to maxIterations") {
    object ToFixedPoint extends RuleExecutor[Expression] {
      val batches = Batch("fixedPoint", FixedPoint(10), DecrementLiterals) :: Nil
    }

    val message = intercept[TreeNodeException[LogicalPlan]] {
      ToFixedPoint.execute(Literal(100))
    }.getMessage
    assert(message.contains("Max iterations (10) reached for batch fixedPoint"))
  }
}
