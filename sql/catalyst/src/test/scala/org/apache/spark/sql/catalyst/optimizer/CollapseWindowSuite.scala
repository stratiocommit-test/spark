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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class CollapseWindowSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapseWindow", FixedPoint(10),
        CollapseWindow) :: Nil
  }

  val testRelation = LocalRelation('a.double, 'b.double, 'c.string)
  val a = testRelation.output(0)
  val b = testRelation.output(1)
  val c = testRelation.output(2)
  val partitionSpec1 = Seq(c)
  val partitionSpec2 = Seq(c + 1)
  val orderSpec1 = Seq(c.asc)
  val orderSpec2 = Seq(c.desc)

  test("collapse two adjacent windows with the same partition/order") {
    val query = testRelation
      .window(Seq(min(a).as('min_a)), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as('max_a)), partitionSpec1, orderSpec1)
      .window(Seq(sum(b).as('sum_b)), partitionSpec1, orderSpec1)
      .window(Seq(avg(b).as('avg_b)), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation.window(Seq(
      min(a).as('min_a),
      max(a).as('max_a),
      sum(b).as('sum_b),
      avg(b).as('avg_b)), partitionSpec1, orderSpec1)

    comparePlans(optimized, correctAnswer)
  }

  test("Don't collapse adjacent windows with different partitions or orders") {
    val query1 = testRelation
      .window(Seq(min(a).as('min_a)), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as('max_a)), partitionSpec1, orderSpec2)

    val optimized1 = Optimize.execute(query1.analyze)
    val correctAnswer1 = query1.analyze

    comparePlans(optimized1, correctAnswer1)

    val query2 = testRelation
      .window(Seq(min(a).as('min_a)), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as('max_a)), partitionSpec2, orderSpec1)

    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer2 = query2.analyze

    comparePlans(optimized2, correctAnswer2)
  }
}
