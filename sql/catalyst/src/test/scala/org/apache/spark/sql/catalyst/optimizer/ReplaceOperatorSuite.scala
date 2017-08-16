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
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ReplaceOperatorSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Replace Operators", FixedPoint(100),
        ReplaceDistinctWithAggregate,
        ReplaceExceptWithAntiJoin,
        ReplaceIntersectWithSemiJoin) :: Nil
  }

  test("replace Intersect with Left-semi Join") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int)

    val query = Intersect(table1, table2)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Join(table1, table2, LeftSemi, Option('a <=> 'c && 'b <=> 'd))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Except with Left-anti Join") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int)

    val query = Except(table1, table2)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Join(table1, table2, LeftAnti, Option('a <=> 'c && 'b <=> 'd))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Distinct with Aggregate") {
    val input = LocalRelation('a.int, 'b.int)

    val query = Distinct(input)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = Aggregate(input.output, input.output, input)

    comparePlans(optimized, correctAnswer)
  }
}
