/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._


class OptimizeCodegenSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("OptimizeCodegen", Once, OptimizeCodegen(SimpleCatalystConf(true))) :: Nil
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation).analyze)
    comparePlans(actual, correctAnswer)
  }

  test("Codegen only when the number of branches is small.") {
    assertEquivalent(
      CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)),
      CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)).toCodegen())

    assertEquivalent(
      CaseWhen(List.fill(100)(TrueLiteral, Literal(1)), Literal(2)),
      CaseWhen(List.fill(100)(TrueLiteral, Literal(1)), Literal(2)))
  }

  test("Nested CaseWhen Codegen.") {
    assertEquivalent(
      CaseWhen(
        Seq((CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)), Literal(3))),
        CaseWhen(Seq((TrueLiteral, Literal(4))), Literal(5))),
      CaseWhen(
        Seq((CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)).toCodegen(), Literal(3))),
        CaseWhen(Seq((TrueLiteral, Literal(4))), Literal(5)).toCodegen()).toCodegen())
  }

  test("Multiple CaseWhen in one operator.") {
    val plan = OneRowRelation
      .select(
        CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)),
        CaseWhen(Seq((FalseLiteral, Literal(3))), Literal(4)),
        CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)),
        CaseWhen(Seq((TrueLiteral, Literal(5))), Literal(6))).analyze
    val correctAnswer = OneRowRelation
      .select(
        CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)).toCodegen(),
        CaseWhen(Seq((FalseLiteral, Literal(3))), Literal(4)).toCodegen(),
        CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)),
        CaseWhen(Seq((TrueLiteral, Literal(5))), Literal(6)).toCodegen()).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, correctAnswer)
  }

  test("Multiple CaseWhen in different operators") {
    val plan = OneRowRelation
      .select(
        CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)),
        CaseWhen(Seq((FalseLiteral, Literal(3))), Literal(4)),
        CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)))
      .where(
        LessThan(
          CaseWhen(Seq((TrueLiteral, Literal(5))), Literal(6)),
          CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)))
      ).analyze
    val correctAnswer = OneRowRelation
      .select(
        CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)).toCodegen(),
        CaseWhen(Seq((FalseLiteral, Literal(3))), Literal(4)).toCodegen(),
        CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)))
      .where(
        LessThan(
          CaseWhen(Seq((TrueLiteral, Literal(5))), Literal(6)).toCodegen(),
          CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)))
      ).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, correctAnswer)
  }
}
