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
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class BooleanSimplificationSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("Constant Folding", FixedPoint(50),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        PruneFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int, 'd.string)

  private def checkCondition(input: Expression, expected: Expression): Unit = {
    val plan = testRelation.where(input).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = testRelation.where(expected).analyze
    comparePlans(actual, correctAnswer)
  }

  test("a && a => a") {
    checkCondition(Literal(1) < 'a && Literal(1) < 'a, Literal(1) < 'a)
    checkCondition(Literal(1) < 'a && Literal(1) < 'a && Literal(1) < 'a, Literal(1) < 'a)
  }

  test("a || a => a") {
    checkCondition(Literal(1) < 'a || Literal(1) < 'a, Literal(1) < 'a)
    checkCondition(Literal(1) < 'a || Literal(1) < 'a || Literal(1) < 'a, Literal(1) < 'a)
  }

  test("(a && b && c && ...) || (a && b && d && ...) || (a && b && e && ...) ...") {
    checkCondition('b > 3 || 'c > 5, 'b > 3 || 'c > 5)

    checkCondition(('a < 2 && 'a > 3 && 'b > 5) || 'a < 2, 'a < 2)

    checkCondition('a < 2 || ('a < 2 && 'a > 3 && 'b > 5), 'a < 2)

    val input = ('a === 'b && 'b > 3 && 'c > 2) ||
      ('a === 'b && 'c < 1 && 'a === 5) ||
      ('a === 'b && 'b < 5 && 'a > 1)

    val expected = 'a === 'b && (
      ('b > 3 && 'c > 2) || ('c < 1 && 'a === 5) || ('b < 5 && 'a > 1))

    checkCondition(input, expected)
  }

  test("(a || b || c || ...) && (a || b || d || ...) && (a || b || e || ...) ...") {
    checkCondition('b > 3 && 'c > 5, 'b > 3 && 'c > 5)

    checkCondition(('a < 2 || 'a > 3 || 'b > 5) && 'a < 2, 'a < 2)

    checkCondition('a < 2 && ('a < 2 || 'a > 3 || 'b > 5), 'a < 2)

    checkCondition(('a < 2 || 'b > 3) && ('a < 2 || 'c > 5), 'a < 2 || ('b > 3 && 'c > 5))

    checkCondition(
      ('a === 'b || 'b > 3) && ('a === 'b || 'a > 3) && ('a === 'b || 'a < 5),
      'a === 'b || 'b > 3 && 'a > 3 && 'a < 5)
  }

  test("a && (!a || b)") {
    checkCondition('a && (!'a || 'b ), 'a && 'b)

    checkCondition('a && ('b || !'a ), 'a && 'b)

    checkCondition((!'a || 'b ) && 'a, 'b && 'a)

    checkCondition(('b || !'a ) && 'a, 'b && 'a)
  }

  test("a < 1 && (!(a < 1) || b)") {
    checkCondition('a < 1 && (!('a < 1) || 'b), ('a < 1) && 'b)
    checkCondition('a < 1 && ('b || !('a < 1)), ('a < 1) && 'b)

    checkCondition('a <= 1 && (!('a <= 1) || 'b), ('a <= 1) && 'b)
    checkCondition('a <= 1 && ('b || !('a <= 1)), ('a <= 1) && 'b)

    checkCondition('a > 1 && (!('a > 1) || 'b), ('a > 1) && 'b)
    checkCondition('a > 1 && ('b || !('a > 1)), ('a > 1) && 'b)

    checkCondition('a >= 1 && (!('a >= 1) || 'b), ('a >= 1) && 'b)
    checkCondition('a >= 1 && ('b || !('a >= 1)), ('a >= 1) && 'b)
  }

  test("a < 1 && ((a >= 1) || b)") {
    checkCondition('a < 1 && ('a >= 1 || 'b ), ('a < 1) && 'b)
    checkCondition('a < 1 && ('b || 'a >= 1), ('a < 1) && 'b)

    checkCondition('a <= 1 && ('a > 1 || 'b ), ('a <= 1) && 'b)
    checkCondition('a <= 1 && ('b || 'a > 1), ('a <= 1) && 'b)

    checkCondition('a > 1 && (('a <= 1) || 'b), ('a > 1) && 'b)
    checkCondition('a > 1 && ('b || ('a <= 1)), ('a > 1) && 'b)

    checkCondition('a >= 1 && (('a < 1) || 'b), ('a >= 1) && 'b)
    checkCondition('a >= 1 && ('b || ('a < 1)), ('a >= 1) && 'b)
  }

  test("DeMorgan's law") {
    checkCondition(!('a && 'b), !'a || !'b)

    checkCondition(!('a || 'b), !'a && !'b)

    checkCondition(!(('a && 'b) || ('c && 'd)), (!'a || !'b) && (!'c || !'d))

    checkCondition(!(('a || 'b) && ('c || 'd)), (!'a && !'b) || (!'c && !'d))
  }

  private val caseInsensitiveConf = new SimpleCatalystConf(false)
  private val caseInsensitiveAnalyzer = new Analyzer(
    new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, caseInsensitiveConf),
    caseInsensitiveConf)

  test("(a && b) || (a && c) => a && (b || c) when case insensitive") {
    val plan = caseInsensitiveAnalyzer.execute(
      testRelation.where(('a > 2 && 'b > 3) || ('A > 2 && 'b < 5)))
    val actual = Optimize.execute(plan)
    val expected = caseInsensitiveAnalyzer.execute(
      testRelation.where('a > 2 && ('b > 3 || 'b < 5)))
    comparePlans(actual, expected)
  }

  test("(a || b) && (a || c) => a || (b && c) when case insensitive") {
    val plan = caseInsensitiveAnalyzer.execute(
      testRelation.where(('a > 2 || 'b > 3) && ('A > 2 || 'b < 5)))
    val actual = Optimize.execute(plan)
    val expected = caseInsensitiveAnalyzer.execute(
      testRelation.where('a > 2 || ('b > 3 && 'b < 5)))
    comparePlans(actual, expected)
  }
}
