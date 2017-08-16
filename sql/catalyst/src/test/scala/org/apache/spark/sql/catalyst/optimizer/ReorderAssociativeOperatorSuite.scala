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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ReorderAssociativeOperatorSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("ReorderAssociativeOperator", Once,
        ReorderAssociativeOperator) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("Reorder associative operators") {
    val originalQuery =
      testRelation
        .select(
          (Literal(3) + ((Literal(1) + 'a) + 2)) + 4,
          'b * 1 * 2 * 3 * 4,
          ('b + 1) * 2 * 3 * 4,
          'a + 1 + 'b + 2 + 'c + 3,
          'a + 1 + 'b * 2 + 'c + 3,
          Rand(0) * 1 * 2 * 3 * 4)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          ('a + 10).as("((3 + ((1 + a) + 2)) + 4)"),
          ('b * 24).as("((((b * 1) * 2) * 3) * 4)"),
          (('b + 1) * 24).as("((((b + 1) * 2) * 3) * 4)"),
          ('a + 'b + 'c + 6).as("(((((a + 1) + b) + 2) + c) + 3)"),
          ('a + 'b * 2 + 'c + 4).as("((((a + 1) + (b * 2)) + c) + 3)"),
          Rand(0) * 1 * 2 * 3 * 4)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nested expression with aggregate operator") {
    val originalQuery =
      testRelation.as("t1")
        .join(testRelation.as("t2"), Inner, Some("t1.a".attr === "t2.a".attr))
        .groupBy("t1.a".attr + 1, "t2.a".attr + 1)(
          (("t1.a".attr + 1) + ("t2.a".attr + 1)).as("col"))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }
}
