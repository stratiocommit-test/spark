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

import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class AggregateOptimizeSuite extends PlanTest {
  val conf = SimpleCatalystConf(caseSensitiveAnalysis = false, groupByOrdinal = false)
  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  val analyzer = new Analyzer(catalog, conf)

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Aggregate", FixedPoint(100),
      FoldablePropagation,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("remove literals in grouping expression") {
    val query = testRelation.groupBy('a, Literal("1"), Literal(1) + Literal(2))(sum('b))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = testRelation.groupBy('a)(sum('b)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not remove all grouping expressions if they are all literals") {
    val query = testRelation.groupBy(Literal("1"), Literal(1) + Literal(2))(sum('b))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(testRelation.groupBy(Literal(0))(sum('b)))

    comparePlans(optimized, correctAnswer)
  }

  test("Remove aliased literals") {
    val query = testRelation.select('a, Literal(1).as('y)).groupBy('a, 'y)(sum('b))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = testRelation.select('a, Literal(1).as('y)).groupBy('a)(sum('b)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("remove repetition in grouping expression") {
    val input = LocalRelation('a.int, 'b.int, 'c.int)
    val query = input.groupBy('a + 1, 'b + 2, Literal(1) + 'A, Literal(2) + 'B)(sum('c))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = input.groupBy('a + 1, 'b + 2)(sum('c)).analyze

    comparePlans(optimized, correctAnswer)
  }
}
