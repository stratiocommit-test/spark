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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, InnerLike, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor


class JoinOptimizationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown", FixedPoint(100),
        CombineFilters,
        PushDownPredicate,
        BooleanSimplification,
        ReorderJoin,
        PushPredicateThroughJoin,
        ColumnPruning,
        CollapseProject) :: Nil

  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int)

  test("extract filters and joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    def testExtract(plan: LogicalPlan, expected: Option[(Seq[LogicalPlan], Seq[Expression])]) {
      val expectedNoCross = expected map {
        seq_pair => {
          val plans = seq_pair._1
          val noCartesian = plans map { plan => (plan, Inner) }
          (noCartesian, seq_pair._2)
        }
      }
      testExtractCheckCross(plan, expectedNoCross)
    }

    def testExtractCheckCross
        (plan: LogicalPlan, expected: Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]) {
      assert(ExtractFiltersAndInnerJoins.unapply(plan) === expected)
    }

    testExtract(x, None)
    testExtract(x.where("x.b".attr === 1), None)
    testExtract(x.join(y), Some(Seq(x, y), Seq()))
    testExtract(x.join(y, condition = Some("x.b".attr === "y.d".attr)),
      Some(Seq(x, y), Seq("x.b".attr === "y.d".attr)))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr),
      Some(Seq(x, y), Seq("x.b".attr === "y.d".attr)))
    testExtract(x.join(y).join(z), Some(Seq(x, y, z), Seq()))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr).join(z),
      Some(Seq(x, y, z), Seq("x.b".attr === "y.d".attr)))
    testExtract(x.join(y).join(x.join(z)), Some(Seq(x, y, x.join(z)), Seq()))
    testExtract(x.join(y).join(x.join(z)).where("x.b".attr === "y.d".attr),
      Some(Seq(x, y, x.join(z)), Seq("x.b".attr === "y.d".attr)))

    testExtractCheckCross(x.join(y, Cross), Some(Seq((x, Cross), (y, Cross)), Seq()))
    testExtractCheckCross(x.join(y, Cross).join(z, Cross),
      Some(Seq((x, Cross), (y, Cross), (z, Cross)), Seq()))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some(Seq((x, Cross), (y, Cross), (z, Cross)), Seq("x.b".attr === "y.d".attr)))
    testExtractCheckCross(x.join(y, Inner, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some(Seq((x, Inner), (y, Inner), (z, Cross)), Seq("x.b".attr === "y.d".attr)))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Inner),
      Some(Seq((x, Cross), (y, Cross), (z, Inner)), Seq("x.b".attr === "y.d".attr)))
  }

  test("reorder inner joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    val queryAnswers = Seq(
      (
        x.join(y).join(z).where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, condition = Some("x.b".attr === "z.b".attr))
          .join(y, condition = Some("y.d".attr === "z.a".attr))
      ),
      (
        x.join(y, Cross).join(z, Cross)
          .where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, Cross, Some("x.b".attr === "z.b".attr))
          .join(y, Cross, Some("y.d".attr === "z.a".attr))
      ),
      (
        x.join(y, Inner).join(z, Cross).where("x.b".attr === "z.a".attr),
        x.join(z, Cross, Some("x.b".attr === "z.a".attr)).join(y, Inner)
      )
    )

    queryAnswers foreach { queryAnswerPair =>
      val optimized = Optimize.execute(queryAnswerPair._1.analyze)
      comparePlans(optimized, analysis.EliminateSubqueryAliases(queryAnswerPair._2.analyze))
    }
  }

  test("broadcasthint sets relation statistics to smallest value") {
    val input = LocalRelation('key.int, 'value.string)

    val query =
      Project(Seq($"x.key", $"y.key"),
        Join(
          SubqueryAlias("x", input, None),
          BroadcastHint(SubqueryAlias("y", input, None)), Cross, None)).analyze

    val optimized = Optimize.execute(query)

    val expected =
      Join(
        Project(Seq($"x.key"), SubqueryAlias("x", input, None)),
        BroadcastHint(Project(Seq($"y.key"), SubqueryAlias("y", input, None))),
        Cross, None).analyze

    comparePlans(optimized, expected)

    val broadcastChildren = optimized.collect {
      case Join(_, r, _, _) if r.statistics.sizeInBytes == 1 => r
    }
    assert(broadcastChildren.size == 1)
  }
}
