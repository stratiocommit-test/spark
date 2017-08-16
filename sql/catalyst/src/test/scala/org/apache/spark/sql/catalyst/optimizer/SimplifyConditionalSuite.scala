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

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.{IntegerType, NullType}


class SimplifyConditionalSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("SimplifyConditionals", FixedPoint(50), SimplifyConditionals) :: Nil
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation).analyze)
    comparePlans(actual, correctAnswer)
  }

  private val trueBranch = (TrueLiteral, Literal(5))
  private val normalBranch = (NonFoldableLiteral(true), Literal(10))
  private val unreachableBranch = (FalseLiteral, Literal(20))
  private val nullBranch = (Literal.create(null, NullType), Literal(30))

  test("simplify if") {
    assertEquivalent(
      If(TrueLiteral, Literal(10), Literal(20)),
      Literal(10))

    assertEquivalent(
      If(FalseLiteral, Literal(10), Literal(20)),
      Literal(20))

    assertEquivalent(
      If(Literal.create(null, NullType), Literal(10), Literal(20)),
      Literal(20))
  }

  test("remove unreachable branches") {
    // i.e. removing branches whose conditions are always false
    assertEquivalent(
      CaseWhen(unreachableBranch :: normalBranch :: unreachableBranch :: nullBranch :: Nil, None),
      CaseWhen(normalBranch :: Nil, None))
  }

  test("remove entire CaseWhen if only the else branch is reachable") {
    assertEquivalent(
      CaseWhen(unreachableBranch :: unreachableBranch :: nullBranch :: Nil, Some(Literal(30))),
      Literal(30))

    assertEquivalent(
      CaseWhen(unreachableBranch :: unreachableBranch :: Nil, None),
      Literal.create(null, IntegerType))
  }

  test("remove entire CaseWhen if the first branch is always true") {
    assertEquivalent(
      CaseWhen(trueBranch :: normalBranch :: nullBranch :: Nil, None),
      Literal(5))

    // Test branch elimination and simplification in combination
    assertEquivalent(
      CaseWhen(unreachableBranch :: unreachableBranch :: nullBranch :: trueBranch :: normalBranch
        :: Nil, None),
      Literal(5))

    // Make sure this doesn't trigger if there is a non-foldable branch before the true branch
    assertEquivalent(
      CaseWhen(normalBranch :: trueBranch :: normalBranch :: Nil, None),
      CaseWhen(normalBranch :: trueBranch :: normalBranch :: Nil, None))
  }
}
