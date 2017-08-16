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

import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._


class EliminateSubqueryAliasesSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("EliminateSubqueryAliases", Once, EliminateSubqueryAliases) :: Nil
  }

  private def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation).analyze)
    comparePlans(actual, correctAnswer)
  }

  private def afterOptimization(plan: LogicalPlan): LogicalPlan = {
    Optimize.execute(analysis.SimpleAnalyzer.execute(plan))
  }

  test("eliminate top level subquery") {
    val input = LocalRelation('a.int, 'b.int)
    val query = SubqueryAlias("a", input, None)
    comparePlans(afterOptimization(query), input)
  }

  test("eliminate mid-tree subquery") {
    val input = LocalRelation('a.int, 'b.int)
    val query = Filter(TrueLiteral, SubqueryAlias("a", input, None))
    comparePlans(
      afterOptimization(query),
      Filter(TrueLiteral, LocalRelation('a.int, 'b.int)))
  }

  test("eliminate multiple subqueries") {
    val input = LocalRelation('a.int, 'b.int)
    val query = Filter(TrueLiteral,
      SubqueryAlias("c", SubqueryAlias("b", SubqueryAlias("a", input, None), None), None))
    comparePlans(
      afterOptimization(query),
      Filter(TrueLiteral, LocalRelation('a.int, 'b.int)))
  }
}
