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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class BinaryComparisonSimplificationSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("Constant Folding", FixedPoint(50),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        SimplifyBinaryComparison,
        PruneFilters) :: Nil
  }

  val nullableRelation = LocalRelation('a.int.withNullability(true))
  val nonNullableRelation = LocalRelation('a.int.withNullability(false))

  test("Preserve nullable exprs in general") {
    for (e <- Seq('a === 'a, 'a <= 'a, 'a >= 'a, 'a < 'a, 'a > 'a)) {
      val plan = nullableRelation.where(e).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = plan
      comparePlans(actual, correctAnswer)
    }
  }

  test("Preserve non-deterministic exprs") {
    val plan = nonNullableRelation
      .where(Rand(0) === Rand(0) && Rand(1) <=> Rand(1)).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = plan
    comparePlans(actual, correctAnswer)
  }

  test("Nullable Simplification Primitive: <=>") {
    val plan = nullableRelation.select('a <=> 'a).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = nullableRelation.select(Alias(TrueLiteral, "(a <=> a)")()).analyze
    comparePlans(actual, correctAnswer)
  }

  test("Non-Nullable Simplification Primitive") {
    val plan = nonNullableRelation
      .select('a === 'a, 'a <=> 'a, 'a <= 'a, 'a >= 'a, 'a < 'a, 'a > 'a).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = nonNullableRelation
      .select(
        Alias(TrueLiteral, "(a = a)")(),
        Alias(TrueLiteral, "(a <=> a)")(),
        Alias(TrueLiteral, "(a <= a)")(),
        Alias(TrueLiteral, "(a >= a)")(),
        Alias(FalseLiteral, "(a < a)")(),
        Alias(FalseLiteral, "(a > a)")())
      .analyze
    comparePlans(actual, correctAnswer)
  }

  test("Expression Normalization") {
    val plan = nonNullableRelation.where(
      'a * Literal(100) + Pi() === Pi() + Literal(100) * 'a &&
      DateAdd(CurrentDate(), 'a + Literal(2)) <= DateAdd(CurrentDate(), Literal(2) + 'a))
      .analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = nonNullableRelation.analyze
    comparePlans(actual, correctAnswer)
  }
}
