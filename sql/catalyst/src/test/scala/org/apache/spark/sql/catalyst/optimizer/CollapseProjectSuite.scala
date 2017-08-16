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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Rand
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class CollapseProjectSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", FixedPoint(10), EliminateSubqueryAliases) ::
      Batch("CollapseProject", Once, CollapseProject) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int)

  test("collapse two deterministic, independent projects into one") {
    val query = testRelation
      .select(('a + 1).as('a_plus_1), 'b)
      .select('a_plus_1, ('b + 1).as('b_plus_1))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.select(('a + 1).as('a_plus_1), ('b + 1).as('b_plus_1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two deterministic, dependent projects into one") {
    val query = testRelation
      .select(('a + 1).as('a_plus_1), 'b)
      .select(('a_plus_1 + 1).as('a_plus_2), 'b)

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation.select(
      (('a + 1).as('a_plus_1) + 1).as('a_plus_2),
      'b).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse nondeterministic projects") {
    val query = testRelation
      .select(Rand(10).as('rand))
      .select(('rand + 1).as('rand1), ('rand + 2).as('rand2))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two nondeterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as('rand))
      .select(Rand(20).as('rand2))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(Rand(20).as('rand2)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse one nondeterministic, one deterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as('rand), 'a)
      .select(('a + 1).as('a_plus_1))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(('a + 1).as('a_plus_1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse project into aggregate") {
    val query = testRelation
      .groupBy('a, 'b)(('a + 1).as('a_plus_1), 'b)
      .select('a_plus_1, ('b + 1).as('b_plus_1))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .groupBy('a, 'b)(('a + 1).as('a_plus_1), ('b + 1).as('b_plus_1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse common nondeterministic project and aggregate") {
    val query = testRelation
      .groupBy('a)('a, Rand(10).as('rand))
      .select(('rand + 1).as('rand1), ('rand + 2).as('rand2))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }
}
