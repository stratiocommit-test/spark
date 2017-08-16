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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class PruneFiltersSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown and Pruning", Once,
        CombineFilters,
        PruneFilters,
        PushDownPredicate,
        PushPredicateThroughJoin) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("Constraints of isNull + LeftOuter") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val query = x.where("x.b".attr.isNull).join(y, LeftOuter)
    val queryWithUselessFilter = query.where("x.b".attr.isNull)

    val optimized = Optimize.execute(queryWithUselessFilter.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constraints of unionall") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int)
    val tr2 = LocalRelation('d.int, 'e.int, 'f.int)
    val tr3 = LocalRelation('g.int, 'h.int, 'i.int)

    val query =
      tr1.where('a.attr > 10)
        .union(tr2.where('d.attr > 10)
        .union(tr3.where('g.attr > 10)))
    val queryWithUselessFilter = query.where('a.attr > 10)

    val optimized = Optimize.execute(queryWithUselessFilter.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Pruning multiple constraints in the same run") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('a.int, 'd.int, 'e.int).subquery('tr2)

    val query = tr1
      .where("tr1.a".attr > 10 || "tr1.c".attr < 10)
      .join(tr2.where('d.attr < 100), Inner, Some("tr1.a".attr === "tr2.a".attr))
    // different order of "tr2.a" and "tr1.a"
    val queryWithUselessFilter =
      query.where(
        ("tr1.a".attr > 10 || "tr1.c".attr < 10) &&
          'd.attr < 100 &&
          "tr2.a".attr === "tr1.a".attr)

    val optimized = Optimize.execute(queryWithUselessFilter.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Partial pruning") {
    val tr1 = LocalRelation('a.int, 'b.int, 'c.int).subquery('tr1)
    val tr2 = LocalRelation('a.int, 'd.int, 'e.int).subquery('tr2)

    // One of the filter condition does not exist in the constraints of its child
    // Thus, the filter is not removed
    val query = tr1
      .where("tr1.a".attr > 10)
      .join(tr2.where('d.attr < 100), Inner, Some("tr1.a".attr === "tr2.d".attr))
    val queryWithExtraFilters =
      query.where("tr1.a".attr > 10 && 'd.attr < 100 && "tr1.a".attr === "tr2.a".attr)

    val optimized = Optimize.execute(queryWithExtraFilters.analyze)
    val correctAnswer = tr1
      .where("tr1.a".attr > 10)
      .join(tr2.where('d.attr < 100),
        Inner,
        Some("tr1.a".attr === "tr2.a".attr && "tr1.a".attr === "tr2.d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("No predicate is pruned") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val query = x.where("x.b".attr.isNull).join(y, LeftOuter)
    val queryWithExtraFilters = query.where("x.b".attr.isNotNull)

    val optimized = Optimize.execute(queryWithExtraFilters.analyze)
    val correctAnswer =
      testRelation.where("b".attr.isNull).where("b".attr.isNotNull)
        .join(testRelation, LeftOuter).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Nondeterministic predicate is not pruned") {
    val originalQuery = testRelation.where(Rand(10) > 5).select('a).where(Rand(10) > 5).analyze
    val optimized = Optimize.execute(originalQuery)
    val correctAnswer = testRelation.where(Rand(10) > 5).where(Rand(10) > 5).select('a).analyze
    comparePlans(optimized, correctAnswer)
  }
}
