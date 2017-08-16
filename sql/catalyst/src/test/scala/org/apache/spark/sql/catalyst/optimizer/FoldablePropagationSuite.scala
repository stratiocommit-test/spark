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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class FoldablePropagationSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Foldable Propagation", FixedPoint(20),
        FoldablePropagation) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int)

  test("Propagate from subquery") {
    val query = OneRowRelation
      .select(Literal(1).as('a), Literal(2).as('b))
      .subquery('T)
      .select('a, 'b)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = OneRowRelation
      .select(Literal(1).as('a), Literal(2).as('b))
      .subquery('T)
      .select(Literal(1).as('a), Literal(2).as('b)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to select clause") {
    val query = testRelation
      .select('a.as('x), "str".as('y), 'b.as('z))
      .select('x, 'y, 'z)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select('a.as('x), "str".as('y), 'b.as('z))
      .select('x, "str".as('y), 'z).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to where clause") {
    val query = testRelation
      .select("str".as('y))
      .where('y === "str" && "str" === 'y)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select("str".as('y))
      .where("str".as('y) === "str" && "str" === "str".as('y)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to orderBy clause") {
    val query = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .orderBy('x.asc, 'y.asc, 'b.desc)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .orderBy('x.asc, SortOrder(Year(CurrentDate()), Ascending), 'b.desc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to groupBy clause") {
    val query = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .groupBy('x, 'y, 'b)(sum('x), avg('y).as('AVG), count('b))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .groupBy('x, Year(CurrentDate()).as('y), 'b)(sum('x), avg(Year(CurrentDate())).as('AVG),
        count('b)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in a complex query") {
    val query = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .where('x > 1 && 'y === 2016 && 'b > 1)
      .groupBy('x, 'y, 'b)(sum('x), avg('y).as('AVG), count('b))
      .orderBy('x.asc, 'AVG.asc)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .where('x > 1 && Year(CurrentDate()).as('y) === 2016 && 'b > 1)
      .groupBy('x, Year(CurrentDate()).as("y"), 'b)(sum('x), avg(Year(CurrentDate())).as('AVG),
        count('b))
      .orderBy('x.asc, 'AVG.asc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in subqueries of Union queries") {
    val query = Union(
      Seq(
        testRelation.select(Literal(1).as('x), 'a).select('x, 'x + 'a),
        testRelation.select(Literal(2).as('x), 'a).select('x, 'x + 'a)))
      .select('x)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Union(
      Seq(
        testRelation.select(Literal(1).as('x), 'a)
          .select(Literal(1).as('x), (Literal(1).as('x) + 'a).as("(x + a)")),
        testRelation.select(Literal(2).as('x), 'a)
          .select(Literal(2).as('x), (Literal(2).as('x) + 'a).as("(x + a)"))))
      .select('x).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in expand") {
    val c1 = Literal(1).as('a)
    val c2 = Literal(2).as('b)
    val a1 = c1.toAttribute.withNullability(true)
    val a2 = c2.toAttribute.withNullability(true)
    val expand = Expand(
      Seq(Seq(Literal(null), 'b), Seq('a, Literal(null))),
      Seq(a1, a2),
      OneRowRelation.select(c1, c2))
    val query = expand.where(a1.isNotNull).select(a1, a2).analyze
    val optimized = Optimize.execute(query)
    val correctExpand = expand.copy(projections = Seq(
      Seq(Literal(null), c2),
      Seq(c1, Literal(null))))
    val correctAnswer = correctExpand.where(a1.isNotNull).select(a1, a2).analyze
    comparePlans(optimized, correctAnswer)
  }
}
