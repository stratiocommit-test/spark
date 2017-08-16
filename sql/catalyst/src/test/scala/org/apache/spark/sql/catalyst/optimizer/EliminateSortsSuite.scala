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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class EliminateSortsSuite extends PlanTest {
  val conf = new SimpleCatalystConf(caseSensitiveAnalysis = true, orderByOrdinal = false)
  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  val analyzer = new Analyzer(catalog, conf)

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Eliminate Sorts", FixedPoint(10),
        FoldablePropagation,
        EliminateSorts) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("Empty order by clause") {
    val x = testRelation

    val query = x.orderBy()
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = x.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("All the SortOrder are no-op") {
    val x = testRelation

    val query = x.orderBy(SortOrder(3, Ascending), SortOrder(-1, Ascending))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(x)

    comparePlans(optimized, correctAnswer)
  }

  test("Partial order-by clauses contain no-op SortOrder") {
    val x = testRelation

    val query = x.orderBy(SortOrder(3, Ascending), 'a.asc)
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(x.orderBy('a.asc))

    comparePlans(optimized, correctAnswer)
  }

  test("Remove no-op alias") {
    val x = testRelation

    val query = x.select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .orderBy('x.asc, 'y.asc, 'b.desc)
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(
      x.select('a.as('x), Year(CurrentDate()).as('y), 'b).orderBy('x.asc, 'b.desc))

    comparePlans(optimized, correctAnswer)
  }
}
